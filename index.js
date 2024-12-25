import fs from 'fs/promises';
import { createReadStream, createWriteStream } from 'fs';
import path from 'path';
import { parse } from 'csv-parse';
import vCard from 'vcf';
import { Transform } from 'stream';
import { pipeline } from 'stream/promises';
import { EventEmitter } from 'events';
import XLSX from 'xlsx';

const masterContacts = new Map();
const compareContacts = new Map();
// Some of the phone numbers from the vcf file are coming big because of the same number repeated multiple times or multiple tel fields

EventEmitter.defaultMaxListeners = 50;
const BATCH_SIZE = 1000;

// Phone column names for different formats
const PHONE_COLUMN_NAMES = [
  'phone',
  'phone_number',
  'mobile',
  'cell',
  'telephone',
  'tel',
  'contact',
  'kc_phone',
];

async function* walkDirectory(dir) {
  const files = await fs.readdir(dir);
  for (const file of files) {
    const filePath = path.join(dir, file);
    const stat = await fs.stat(filePath);
    if (stat.isDirectory()) {
      yield* walkDirectory(filePath);
    } else {
      yield filePath;
    }
  }
}

function standardizePhoneNumber(phone) {
  if (!phone) return '';

  if (Array.isArray(phone)) {
    phone = phone[0];
  }

  // Clean the string to only digits and plus sign
  let cleanNumber = phone.toString().replace(/[^\d+]/g, '');

  // If the number is longer than 15 digits (max valid length for phone numbers)
  // Take the first valid phone number segment (usually 10-12 digits)
  if (cleanNumber.length > 15) {
    // Match the first occurrence of a valid phone number pattern
    // This looks for 10-12 digit sequences, optionally starting with +
    const match = cleanNumber.match(/(?:\+?\d{10,12})/);
    return match ? match[0] : '';
  }

  // if cleanNumber is greater than 10 and has 91 at the start then add + sign

  if (cleanNumber.length > 10) {
    const prefixToRemove = ['91', '0', '00', '+91', '091'];
    prefixToRemove.forEach((prefix) => {
      if (cleanNumber.startsWith(prefix)) {
        cleanNumber = cleanNumber.replace(prefix, '');
      }
    });
  }
  if (cleanNumber.length < 10) {
    return '';
  }

  return cleanNumber;
}

function addToCompareContacts(phone, contact) {
  // If the phone number is not already in the map or if it's the same as the contact's name, add it
  // so that the correct name taken
  const c1 = !compareContacts.has(phone);
  const c2 = phone !== contact.name;
  if (c1 || c2) {
    compareContacts.set(phone, contact);
  }
}

function createCsvStream(filePath) {
  return createReadStream(filePath, { highWaterMark: BATCH_SIZE * 1024 }).pipe(
    parse({
      columns: true,
      skip_empty_lines: true,
      trim: true,
      relax_column_count: true,
      relax_quotes: true,
      quote: '"',
      escape: '"',
      bom: true,
      skip_records_with_error: true,
      batchSize: BATCH_SIZE,
    })
  );
}
async function* readXlsxFile(filePath) {
  const workbook = XLSX.readFile(filePath);
  const worksheet = workbook.Sheets[workbook.SheetNames[0]];
  const data = XLSX.utils.sheet_to_json(worksheet);

  let batch = [];
  for (const row of data) {
    batch.push(row);
    if (batch.length >= BATCH_SIZE) {
      yield* batch;
      batch = [];
    }
  }
  if (batch.length > 0) {
    yield* batch;
  }
}

async function* readVcfFileStream(filePath) {
  const content = await fs.readFile(filePath, 'utf-8');
  const cards = content.split('BEGIN:VCARD');
  let batch = [];

  for (const card of cards) {
    if (card.trim()) {
      const parsedCard = vCard.parse('BEGIN:VCARD' + card)[0];
      // Get all telephone numbers from the card
      const telValues = parsedCard.get('tel');

      if (Array.isArray(telValues)) {
        // Handle multiple phone numbers
        telValues.forEach((tel) => {
          const phoneNumber = standardizePhoneNumber(tel.valueOf());
          if (phoneNumber) {
            parsedCard.set('singleTel', phoneNumber);
            batch.push(parsedCard);
          }
        });
      } else if (telValues) {
        // Handle single phone number
        const phoneNumber = standardizePhoneNumber(telValues.valueOf());
        if (phoneNumber) {
          parsedCard.set('singleTel', phoneNumber);
          batch.push(parsedCard);
        }
      }

      if (batch.length >= BATCH_SIZE) {
        yield* batch;
        batch = [];
      }
    }
  }
  if (batch.length > 0) {
    yield* batch;
  }
}

function prettyJSON(obj) {
  console.log(JSON.stringify(obj, null, 2));
}

function createContactKey(contact, fileName) {
  let phone = '';
  let name = '';

  // For VCF format
  if (contact.get) {
    if (contact.singleTel || contact.get('singleTel')?.valueOf()) {
      phone = contact.singleTel ?? contact.get('singleTel').valueOf();
    } else if (contact.get('tel')) {
      phone = standardizePhoneNumber(contact.get('tel').valueOf());
    }
    if (contact.get('fn')) {
      name = contact.get('fn').valueOf();
      if (Array.isArray(name)) {
        name = name[0].valueOf();
      }
    }

    return { phone, name };
  }
  // For CSV/Excel format
  for (const fieldName of PHONE_COLUMN_NAMES) {
    if (contact[fieldName]) {
      phone = standardizePhoneNumber(contact[fieldName]);
      name =
        contact.kc_name ||
        contact.name ||
        contact.full_name ||
        contact.contact_name ||
        phone;
      if (!(phone && name)) {
        // TODO('Missing phone or name for contact:', { contact, name, phone, fieldName });
      }
      break;
    }
  }
  return { phone, name };
}

class ContactProcessor extends EventEmitter {
  constructor(masterDir, compareDir, outputPath) {
    super();
    this.masterDir = masterDir;
    this.compareDir = compareDir;
    this.outputPath = outputPath;
    this.stats = {
      processed: 0,
      missing: 0,
      errors: 0,
      totalFiles: 0,
    };
  }

  async process() {
    // Process master directory
    for await (const file of walkDirectory(this.masterDir)) {
      const ext = path.extname(file).toLowerCase();
      try {
        this.stats.totalFiles++;
        switch (ext) {
          case '.vcf':
            for await (const contact of readVcfFileStream(file)) {
              const c = createContactKey(contact, file);
              masterContacts.set(c.phone, c);
            }
            break;
          case '.csv':
            for await (const contact of createCsvStream(file)) {
              const c = createContactKey(contact, file);
              masterContacts.set(c.phone, c);
            }
            break;
          case '.xlsx':
          case '.xls':
            for await (const contact of readXlsxFile(file)) {
              const c = createContactKey(contact, file);
              masterContacts.set(c.phone, c);
            }
            break;
        }
        this.stats.processed++;
        this.emit('progress', `Processed ${file}`);
      } catch (error) {
        this.stats.errors++;
        this.emit('error', `Error processing ${file}: ${error.message}`);
      }
    }

    // Process comparison directory
    for await (const file of walkDirectory(this.compareDir)) {
      const ext = path.extname(file).toLowerCase();
      try {
        this.stats.totalFiles++;
        switch (ext) {
          case '.vcf':
            for await (const contact of readVcfFileStream(file)) {
              const c = createContactKey(contact, file);
              addToCompareContacts(c.phone, c);
            }
            break;
          case '.csv':
            for await (const contact of createCsvStream(file)) {
              const c = createContactKey(contact, file);
              addToCompareContacts(c.phone, c);
            }
            break;
          case '.xlsx':
          case '.xls':
            for await (const contact of readXlsxFile(file)) {
              const c = createContactKey(contact, file);
              addToCompareContacts(c.phone, c);
            }
            break;
        }
        this.emit('progress', `Processed comparison file ${file}`);
      } catch (error) {
        this.stats.errors++;
        this.emit(
          'error',
          `Error processing comparison ${file}: ${error.message}`
        );
      }
    }

    // Add detailed logging for the comparison
    // console.log('Master Contacts Sample:', [...masterContacts].slice(0, 5));
    // console.log('Compare Contacts Sample:', [...compareContacts].slice(0, 5));

    const missingContacts = new Set();

    const allContacts = [...compareContacts];
    allContacts.map(([phone, contact]) => {
      // get phone number without + sign and extra char only last 10 digits

      const cleanPhone = standardizePhoneNumber(phone).toString().slice(-10);
      // comparing the contact here
      const isMissing1 =
        !masterContacts.has(phone) && !missingContacts.has(phone);
      const isMissing2 =
        !masterContacts.has(cleanPhone) && !missingContacts.has(cleanPhone);
      const isMissing = isMissing1 && isMissing2;
      if (isMissing) {
        // console.log('Missing Contact:', x);
        missingContacts.add(contact);
      }
      return isMissing;
    });

    console.table({
      masterContacts: masterContacts.size,
      compareContacts: compareContacts.size,
      missingContacts: missingContacts.size,
    });
    // Output missing contacts to VCF
    const writer = createWriteStream(this.outputPath);
    for (const contact of missingContacts) {
      const vcard = new vCard();
      vcard.add('tel', contact);
      vcard.add('fn', contact); // Adding the contact name as the number for now
      vcard.add('n', contact); // Adding structured name
      writer.write(vcard.toString() + '\n');
      this.stats.missing++;
    }
    writer.end();
    // After collecting contacts
    await fs.writeFile(
      'master_numbers.json',
      JSON.stringify([...masterContacts], null, 2)
    );
    await fs.writeFile(
      'compare_numbers.json',
      JSON.stringify([...compareContacts], null, 2)
    );
    await fs.writeFile(
      'missing_numbers.json',
      JSON.stringify([...missingContacts], null, 2)
    );

    return this.stats;
  }
}

async function main() {
  const processor = new ContactProcessor(
    './master_files',
    './compare_files',
    './missing-contacts.vcf'
  );

  processor.on('error', console.error);
  processor.on('progress', console.log);

  try {
    const stats = await processor.process();
    console.log('Processing complete:', {
      totalFiles: stats.totalFiles,
      processedFiles: stats.processed,
      missingContacts: stats.missing,
      errors: stats.errors,
    });
  } catch (error) {
    console.error('Fatal error:', error);
    process.exit(1);
  }
}

main();

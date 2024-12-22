import fs from 'fs/promises';
import { createReadStream, createWriteStream } from 'fs';
import path from 'path';
import { parse } from 'csv-parse';
import vCard from 'vcf';
import { Transform } from 'stream';
import { pipeline } from 'stream/promises';
import { EventEmitter } from 'events';
import XLSX from 'xlsx';

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
  return phone.toString().replace(/[^\d+]/g, '');
}

async function* readVcfFileStream(filePath) {
  const content = await fs.readFile(filePath, 'utf-8');
  const cards = content.split('BEGIN:VCARD');
  let batch = [];

  for (const card of cards) {
    if (card.trim()) {
      batch.push(vCard.parse('BEGIN:VCARD' + card)[0]);
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
      batchSize: BATCH_SIZE
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

function createContactKey(contact) {
  let phone = '';

  // Check all possible phone field names
  for (const fieldName of PHONE_COLUMN_NAMES) {
    if (contact[fieldName]) {
      phone = standardizePhoneNumber(contact[fieldName]);
      // console.log('🚀 ~ createContactKey ~ fieldName:', { fieldName, phone });
      break;
    }
  }

  // For VCF format
  if (contact.get && contact.get('tel')) {
    phone = standardizePhoneNumber(contact.get('tel').valueOf());
  }

  return phone;
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
      totalFiles: 0
    };
  }

  async process() {
    const masterContacts = new Set();
    const compareContacts = new Set();

    // Process master directory
    for await (const file of walkDirectory(this.masterDir)) {
      const ext = path.extname(file).toLowerCase();
      try {
        this.stats.totalFiles++;
        switch (ext) {
          case '.vcf':
            for await (const contact of readVcfFileStream(file)) {
              masterContacts.add(createContactKey(contact));
            }
            break;
          case '.csv':
            for await (const contact of createCsvStream(file)) {
              masterContacts.add(createContactKey(contact));
            }
            break;
          case '.xlsx':
          case '.xls':
            for await (const contact of readXlsxFile(file)) {
              masterContacts.add(createContactKey(contact));
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
              compareContacts.add(createContactKey(contact));
            }
            break;
          case '.csv':
            for await (const contact of createCsvStream(file)) {
              compareContacts.add(createContactKey(contact));
            }
            break;
          case '.xlsx':
          case '.xls':
            for await (const contact of readXlsxFile(file)) {
              compareContacts.add(createContactKey(contact));
            }
            break;
        }
        this.emit('progress', `Processed comparison file ${file}`);
      } catch (error) {
        this.stats.errors++;
        this.emit('error', `Error processing comparison ${file}: ${error.message}`);
      }
    }


    // Find missing contacts
    const missingContacts = new Set(
      [...masterContacts].filter(x => !compareContacts.has(x))
    );

    console.table({
      masterContacts: masterContacts.size,
      compareContacts: compareContacts.size,
      missingContacts: missingContacts.size
    })

    // Output missing contacts to VCF
    const writer = createWriteStream(this.outputPath);
    for (const contact of missingContacts) {
      const vcard = new vCard();
      vcard.add('tel', contact);
      writer.write(vcard.toString());
      this.stats.missing++;
    }
    writer.end();

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
      errors: stats.errors
    });
  } catch (error) {
    console.error('Fatal error:', error);
    process.exit(1);
  }
}

main();

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

// Batch size for processing contacts
const BATCH_SIZE = 1000;

// Modify the readVcfFileStream function to use batching
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

// Modify the CSV stream processing to use batching
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

// Add function to read XLSX files
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
  // Add this at the top of the file with other constants
  const PHONE_COLUMN_NAMES = [
    'phone',
    'phone_number',
    'mobile',
    'mobile_number',
    'contact',
    'contact_number',
    'tel',
    'telephone',
    'cell',
    'cellphone',
    'Phone 1 - Value',
  ];
  let phoneNumber = '';

  if (contact instanceof vCard) {
    const tel = contact.get('tel');
    phoneNumber = tel && tel[0] ? tel[0].valueOf() : '';
  } else {
    // Look through possible column names
    for (const columnName of PHONE_COLUMN_NAMES) {
      if (contact[columnName]) {
        phoneNumber = contact[columnName];
        break;
      }
    }
  }

  // Return early if no phone number is found
  if (!phoneNumber) {
    return '';
  }

  // Extract just the digits for comparison while preserving original number
  const standardizedNumber = phoneNumber.replace(/\D/g, '');

  // Get last 10 digits for comparison (or full number if less than 10)
  return standardizedNumber.slice(-10);
}

// Transform stream for VCF creation
class VcfTransform extends Transform {
  constructor(options = {}) {
    super({ ...options, objectMode: true });
  }

  _transform(contact, encoding, callback) {
    let vcfString;
    if (contact instanceof vCard) {
      vcfString = contact.toString();
    } else {
      const card = new vCard();
      card.set('fn', contact.name);
      card.set('tel', contact.phone);
      vcfString = card.toString();
    }
    callback(null, vcfString + '\n');
  }
}
async function getAllFiles(dirPath) {
  const files = await fs.readdir(dirPath);
  const allFiles = [];

  for (const file of files) {
    const filePath = path.join(dirPath, file);
    const stat = await fs.stat(filePath);

    if (stat.isDirectory()) {
      const subFiles = await getAllFiles(filePath);
      allFiles.push(...subFiles);
    } else {
      allFiles.push(filePath);
    }
  }

  return allFiles;
}

async function processMissingContacts(masterDir, compareDir, outputFile) {
  const masterFiles = await fs.readdir(masterDir);
  const compareFiles = await getAllFiles(compareDir);

  console.log('Master file count:', masterFiles.length);
  console.log('Compare file count:', compareFiles.length);

  const masterContactKeys = new Set();
  const writtenContactKeys = new Set();
  // Add object to track per-file statistics
  const fileStats = {};

  console.log('Processing master files...');
  for (const file of masterFiles) {
    const filePath = path.join(masterDir, file);
    try {
      if (file.endsWith('.vcf')) {
        for await (const contact of readVcfFileStream(filePath)) {
          masterContactKeys.add(createContactKey(contact));
        }
      } else if (file.endsWith('.csv')) {
        const parser = createCsvStream(filePath);
        for await (const contact of parser) {
          masterContactKeys.add(createContactKey(contact));
        }
      } else if (file.endsWith('.xlsx') || file.endsWith('.xls')) {
        for await (const contact of readXlsxFile(filePath)) {
          masterContactKeys.add(createContactKey(contact));
        }
      }
    } catch (error) {
      console.error(`Error processing master file ${file}:`, error.message);
    } finally {
      if (global.gc) {
        global.gc();
      }
    }
  }

  const writeStream = createWriteStream(outputFile);
  writeStream.setMaxListeners(50);

  let missingCount = 0;

  console.log('Processing and comparing contacts...');
  for (const filePath of compareFiles) {
    // Initialize counter for this file
    fileStats[filePath] = 0;

    try {
      const fileExt = path.extname(filePath).toLowerCase();

      if (fileExt === '.vcf') {
        for await (const contact of readVcfFileStream(filePath)) {
          const key = createContactKey(contact);
          if (!masterContactKeys.has(key) && !writtenContactKeys.has(key)) {
            await writeStream.write(contact.toString() + '\n');
            writtenContactKeys.add(key);
            missingCount++;
            fileStats[filePath]++;
          }
        }
      } else if (fileExt === '.csv') {
        const parser = createCsvStream(filePath);
        const vcfTransform = new VcfTransform();

        await pipeline(
          parser,
          new Transform({
            objectMode: true,
            transform(contact, encoding, callback) {
              const key = createContactKey(contact);
              if (!masterContactKeys.has(key) && !writtenContactKeys.has(key)) {
                writtenContactKeys.add(key);
                missingCount++;
                fileStats[filePath]++;
                callback(null, contact);
              } else {
                callback();
              }
            }
          }),
          vcfTransform,
          writeStream
        );
      } else if (fileExt === '.xlsx' || fileExt === '.xls') {
        for await (const contact of readXlsxFile(filePath)) {
          const key = createContactKey(contact);
          if (!masterContactKeys.has(key) && !writtenContactKeys.has(key)) {
            const card = new vCard();
            card.set('fn', contact.name);
            card.set('tel', contact.phone);
            await writeStream.write(card.toString() + '\n');
            writtenContactKeys.add(key);
            missingCount++;
            fileStats[filePath]++;
          }
        }
      }
    } catch (error) {
      console.error(`Error processing compare file $index.js:`, error.message);
    }
  }

  await writeStream.end();

  // Log detailed statistics
  console.log('\nMissing contacts per file:');
  console.log('------------------------');
  for (const [filePath, count] of Object.entries(fileStats)) {
    console.log(`${path.basename(filePath)}: ${count} contacts`);
    if (count > 0) {
    }
  }
  console.log('------------------------');
  console.log(`Process completed. Missing contacts saved to ${outputFile}`);
  console.log(`Total unique missing contacts: ${missingCount}`);
}// Example usage
const masterDir = './master_files';
const compareDir = './compare_files';
const outputFile = './missing_contacts.vcf';

processMissingContacts(masterDir, compareDir, outputFile)
  .catch(error => console.error('Error:', error));

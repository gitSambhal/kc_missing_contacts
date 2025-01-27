import {existsSync, mkdirSync} from 'fs';
import fs from 'fs/promises';
import {createReadStream, createWriteStream} from 'fs';
import path from 'path';
import {parse} from 'csv-parse';
import vCard from 'vcf';
import {EventEmitter} from 'events';
import XLSX from 'xlsx';

const numberToCheck = '';

/**
 * Determines the output format for Excel files.
 *
 * - If `true`, all data will be consolidated into a single output file.
 * - If `false`, data will be split into separate XLSX files.
 */
const outputSingleXlsxFile = true;

/**
 * Determines whether to ensure unique names in the missing contacts list.
 *
 * - If `true`, duplicate names will be resolved by appending a count, starting with (1),
 *   even for the first duplicate (e.g., "John (1)", "John (2)", "John (3)").
 * - If `false`, duplicate names will remain unchanged, preserving their original form.
 */
const makeNameUnique = true;

// Prevent numbers starting with these prefixes in missing list
const blockedPhonePrefixes = ['94544'];

// Prevent numbers ending with these suffixes in missing list
const blockedPhoneSuffixes = ['000000'];

const duplicateMasterContacts = new Map();
const duplicateCompareContacts = new Map();
const duplicateNamesMap = new Map();

const prefixIfNoName = 'KAS';

const FILE_TYPES = {
  VCF: '.vcf',
  CSV: '.csv',
  XLSX: '.xlsx',
};

const filterKeywords = ['spam', 'All Bank Balance Enquiry No'];

const masterContacts = new Map();
const compareContacts = new Map();
// Some of the phone numbers from the vcf file are coming big because of the same number repeated multiple times or multiple tel fields

EventEmitter.defaultMaxListeners = 50;
const BATCH_SIZE = 1000;

// Phone column names for different formats
const PHONE_COLUMN_NAMES = [
  'kc_phone',
  'phone',
  'phone_number',
  'mobile',
  'cell',
  'telephone',
  'tel',
  'contact',
  'phone_value',
  'phone_1_value',
  'phone_2_value',
  'phone_3_value',
  'phone_4_value',
  'phone_5_value',
  'phone_6_value',
  'phone_7_value',
  'phone_8_value',
  'phone_9_value',
  'phone_10_value',
  'phone_11_value',
  'phone_12_value',
  'phone_13_value',
  'phone_14_value',
  'phone_15_value',
  'phone_16_value',
  'car_phone',
  'primary_phone',
  'business_phone',
  'business_phone_2',
  'home_phone',
  'home_phone_2',
  'other_phone',
  'company_main_phone',
];

const nameColumnNames = [
  'kc_name',
  'name',
  'first_name',
  'last_name',
  'given_name',
  'short_name',
  'maiden_name',
  'middle_name',
  'family_name',
  'additional_name',
  'yomi_name',
  'given_name_yomi',
  'additional_name_yomi',
  'family_name_yomi',
  'nickname',
  'real_name',
];

/**
 * Checks if the given string does not contain any numbers.
 *
 * @param {string} value - The string to check.
 * @returns {boolean} - Returns `true` if the string does not contain any number, otherwise `false`.
 */
function hasNoNumber(value) {
  const regex = /^[^0-9]*$/; // Regex to match lines without any numbers
  return regex.test(value);
}

/**
 * Save an array as an Excel file.
 *
 * @param {Array<unknown>} dataArray - The array of objects to save as an Excel file.
 * @param {string} fileName - The name of the file to save as.
 * @param {string} [sheetName] - Optional. The name of the sheet to use in the Excel file. If not provided,
 * the filename will be used as the sheet name.
 * @param {XLSX.WorkBook | null} [workbook] - Optional. The workbook to append the sheet to. If not provided, a new workbook
 * will be created.
 * @param {boolean} [isAppendSheetWithoutSaveFile] - Optional. If true, the sheet will be appended to the workbook without
 * saving the file. If false (default), the workbook will be saved to a file.
 */
function saveArrayAsXlsx(
  dataArray,
  fileName,
  sheetName = '',
  workbook,
  isAppendSheetWithoutSaveFile = false
) {
  // Use filename as sheet name if not provided
  const sheet = sheetName || fileName.replace('.xlsx', '');

  // Create a worksheet from the array
  const worksheet = XLSX.utils.json_to_sheet(dataArray);

  // Create a new workbook/or use passed and append the worksheet
  workbook = workbook || XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(workbook, worksheet, sheet);

  // Write the workbook to a file
  if (!isAppendSheetWithoutSaveFile) {
    XLSX.writeFile(workbook, fileName);
  }
}

function toPhoneNumber(value, filename) {
  if (typeof value !== 'string') return '';

  const prefixToRemove = ['0', '91', '+91', '091', '0091'];
  const invalidPrefix = ['1', '2', '3', '4', '5'];
  let toPhone = value.replace(/[^\d+]/g, '');

  if (toPhone.length > 10) {
    prefixToRemove.forEach((prefix) => {
      if (toPhone.startsWith(prefix)) {
        toPhone = toPhone.replace(prefix, '');
      }
    });
  }

  // if phone is greater then 10 digits and less then 13 and doesn't have a country code then
  if (toPhone.length > 10 && toPhone.length < 13 && !toPhone.startsWith('+')) {
    return '';
  }
  if (toPhone.length > 14 && !toPhone.startsWith('+')) {
    return '';
  }

  const digitCount = toPhone.length;

  const isValidPhone = !!(
    toPhone &&
    digitCount > 9 &&
    !invalidPrefix.includes(String(toPhone[0]))
  );

  return isValidPhone ? toPhone : '';
}

function addToDuplicateMap(map, key) {
  if (typeof key == 'string') {
    key = key.toLowerCase();
  }
  if (!map.has(key)) {
    map.set(key, 1);
  } else {
    map.set(key, map.get(key) + 1);
  }
}

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
  return toPhoneNumber(phone, 'phone');
}

function addToCompareContacts(phone, contact) {
  addToDuplicateMap(duplicateCompareContacts, contact.phone);

  // If the phone number is not already in the map or if it's the same as the contact's name, add it
  // so that the correct name taken

  // use phone number as name if name is not present
  contact.name = contact.name || phone;

  const replaceNamesStartingWith = ['name', 'caller'];

  const isNameZero = '0';
  // add prefix if name is not present with last 5 digits
  if (
    contact.name.includes(phone) ||
    replaceNamesStartingWith.some((name) =>
      String(contact.name).toLowerCase().startsWith(name)
    ) ||
    String(contact.name) == isNameZero
  ) {
    contact.name = `${prefixIfNoName} ${String(phone).slice(-5)}`;
  }
  const cName = contact.name.toString().toLowerCase();
  // Define conditions as an array of each element being true
  const conditions = [
    !compareContacts.has(phone) || phone !== contact.name,
    !filterKeywords.some((name) => cName.includes(name.toLowerCase())),
    Number(phone) > 6_000_000_000,
    !blockedPhonePrefixes.some((prefix) => String(phone).startsWith(prefix)),
    !blockedPhoneSuffixes.some((suffix) => String(phone).endsWith(suffix)),
  ];

  // Check if all conditions are true
  if (conditions.every((condition) => condition)) {
    compareContacts.set(phone, contact);
  }
}

function normalizeColumnName(text) {
  const o = text
    .toLowerCase()
    .toString()
    // Replace multiple spaces with single underscore
    .replace(/\s+/g, '_')
    // Replace multiple dashes with single underscore
    .replace(/-+/g, '_')
    // Replace any other special characters with underscore
    .replace(/[^a-z0-9_]/g, '_')
    // Replace multiple consecutive underscores with single underscore
    .replace(/_+/g, '_')
    // Remove leading and trailing underscores
    .replace(/^_+|_+$/g, '');
  return o;
}

function createCsvStream(filePath) {
  return createReadStream(filePath, {highWaterMark: BATCH_SIZE * 1024}).pipe(
    parse({
      columns: (header) => header.map(normalizeColumnName),
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

function increaseTotalCompareContacts(varToIncr, phoneNo) {
  if (phoneNo) {
    varToIncr.totalCompareContacts++;
  }
}

async function* readXlsxFile(filePath) {
  const workbook = XLSX.readFile(filePath);
  const worksheet = workbook.Sheets[workbook.SheetNames[0]];
  const csvContent = XLSX.utils.sheet_to_csv(worksheet);

  // Create a temporary CSV file
  const tempCsvPath = filePath + '.temp.csv';
  await fs.writeFile(tempCsvPath, csvContent);

  // Use existing createCsvStream function
  const csvStream = createCsvStream(tempCsvPath);

  try {
    for await (const row of csvStream) {
      yield row;
    }
  } finally {
    // Clean up temporary file
    await fs.unlink(tempCsvPath);
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
          const card1 = vCard.parse('BEGIN:VCARD' + card)[0];
          const phoneNumber = standardizePhoneNumber(tel.valueOf());
          if (phoneNumber) {
            card1.set('tel', phoneNumber);
            batch.push(card1);
          }
        });
      } else if (telValues) {
        // Handle single phone number
        const phoneNumber = standardizePhoneNumber(telValues.valueOf());
        if (phoneNumber) {
          parsedCard.set('tel', phoneNumber);
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

function decodeQuotedPrintable(raw, charset = 'utf-8') {
  const dc = new TextDecoder(charset);
  return raw
    .replace(/[\t\x20]$/gm, '')
    .replace(/=(?:\r\n?|\n)/g, '')
    .replace(/((?:=[a-fA-F0-9]{2})+)/g, (m) => {
      const cd = m.substring(1).split('='),
        uArr = new Uint8Array(cd.length);
      for (let i = 0; i < cd.length; i++) {
        uArr[i] = parseInt(cd[i], 16);
      }
      return dc.decode(uArr);
    });
}

function createContactKey(contact, fileName, fileType) {
  if (numberToCheck && JSON.stringify(contact).includes(numberToCheck)) {
    console.log(`found ${numberToCheck} in ${fileName}`);
  }
  let phones = [];
  let name = '';

  // For VCF format
  if (fileType === FILE_TYPES.VCF) {
    if (contact.get) {
      let tels = contact.get('tel');
      if (tels) {
        if (Array.isArray(tels)) {
          for (const tel of tels) {
            const phone = standardizePhoneNumber(tel.valueOf());
            phones.push(phone);
          }
        } else {
          const phone = standardizePhoneNumber(tels.valueOf());
          phones.push(phone);
        }
      }
      if (contact.get('fn')) {
        name = contact.get('fn').valueOf();
        if (Array.isArray(name)) {
          name = name[0].valueOf();
        }
        if (contact.get('fn').encoding === 'QUOTED-PRINTABLE') {
          name = decodeQuotedPrintable(contact.get('fn').valueOf());
        }
      }
    }
  }

  if (fileType === FILE_TYPES.CSV || fileType === FILE_TYPES.XLSX) {
    // For CSV/Excel format
    const columnNames = Object.keys(contact);
    for (const fieldName of columnNames) {
      if (contact[fieldName]) {
        const phone = standardizePhoneNumber(contact[fieldName]);
        if (phone) {
          phones.push(phone);
        }
      }
    }

    // Get name from any matching column
    for (const fieldName of nameColumnNames) {
      if (contact[fieldName]) {
        if (fieldName.includes('first')) {
          name = `${contact[fieldName]} ${contact['last_name']}`.trim();
          break;
        }
        name = contact[fieldName];
        break;
      }
    }
  }

  // clean up name by removing spacees and special characters but keep hindi urdu english characters
  name = cleanupName(name);
  // Return array of contacts, one for each phone number
  return phones.map((phone) => ({phone, name}));
}

function cleanupName(name) {
  const cleanName = name
    .trim()
    // Keep Hindi (Devanagari), Urdu, English letters, numbers, spaces, and common symbols
    .replace(/[^\u0900-\u097F\u0600-\u06FF\w\s\+\/\(\)\[\]]/g, '')
    // Replace multiple spaces with single space
    .replace(/\s+/g, ' ')
    // remove leading and trailing slashes
    .replace(/^\/+|\/+$/g, '')
    .trim();

  if (cleanName != name) {
    // console.log(`Cleaned name: ${cleanName} from ${name}`);
    // const outputName = `${name} -> ${cleanName}`;
    // return outputName;
  }

  return cleanName;
}

class ContactProcessor extends EventEmitter {
  constructor(masterDir, compareDir, outputDir) {
    super();
    this.masterDir = masterDir;
    this.compareDir = compareDir;
    this.outputDir = outputDir;
    this.stats = {
      processed: 0,
      missing: 0,
      errors: 0,
      totalFiles: 0,
      // New stats
      totalMasterContacts: 0,
      uniqueMasterContacts: 0,
      totalCompareContacts: 0,
      uniqueCompareContacts: 0,
    };
    if (!existsSync(this.outputDir)) {
      mkdirSync(this.outputDir);
    }
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
              const contacts = createContactKey(contact, file, FILE_TYPES.VCF);
              contacts.forEach((c) => {
                masterContacts.set(c.phone, c);
                this.stats.totalMasterContacts++;
                addToDuplicateMap(duplicateMasterContacts, c.phone);
              });
            }
            break;
          case '.csv':
            for await (const contact of createCsvStream(file)) {
              const contacts = createContactKey(contact, file, FILE_TYPES.CSV);
              contacts.forEach((c) => {
                masterContacts.set(c.phone, c);
                this.stats.totalMasterContacts++;
                addToDuplicateMap(duplicateMasterContacts, c.phone);
              });
            }
            break;
          case '.xlsx':
          case '.xls':
            for await (const contact of readXlsxFile(file)) {
              const contacts = createContactKey(contact, file, FILE_TYPES.XLSX);
              contacts.forEach((c) => {
                masterContacts.set(c.phone, c);
                this.stats.totalMasterContacts++;
                addToDuplicateMap(duplicateMasterContacts, c.phone);
              });
            }
            break;
        }
        this.stats.processed++;
        // this.emit('progress', `Processed ${file}`);
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
              const contacts = createContactKey(contact, file, FILE_TYPES.VCF);
              contacts.forEach((c) => {
                increaseTotalCompareContacts(this.stats, c.phone);
                addToCompareContacts(c.phone, c);
              });
            }
            break;
          case '.csv':
            for await (const contact of createCsvStream(file)) {
              const contacts = createContactKey(contact, file, FILE_TYPES.CSV);
              // Add each phone number as separate contact
              contacts.forEach((c) => {
                increaseTotalCompareContacts(this.stats, c.phone);
                addToCompareContacts(c.phone, c);
              });
            }
            break;
          case '.xlsx':
          case '.xls':
            for await (const contact of readXlsxFile(file)) {
              const contacts = createContactKey(contact, file, FILE_TYPES.XLSX);
              contacts.forEach((c) => {
                increaseTotalCompareContacts(this.stats, c.phone);
                addToCompareContacts(c.phone, c);
              });
            }
            break;
        }
        // this.emit('progress', `Processed comparison file ${file}`);
      } catch (error) {
        this.stats.errors++;
        this.emit(
          'error',
          `Error processing comparison ${file}: ${error.message} ${error.stack}`
        );
      }
    }

    // Update stats after processing both directories
    this.stats.uniqueMasterContacts = masterContacts.size;
    this.stats.uniqueCompareContacts = compareContacts.size;

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
        addToDuplicateMap(duplicateNamesMap, contact.name);
        missingContacts.add(contact);
      }
      return isMissing;
    });

    // sort missing contacts by name
    const sortedMissingContacts = [...missingContacts].sort((a, b) =>
      a.name.localeCompare(b.name)
    );

    // sort missing contacts by number
    // const sortedMissingContacts = [...missingContacts].sort(
    //   (a, b) => a.phone - b.phone
    // );

    // Output missing contacts to VCF
    const tmpVcfPath = path.join(this.outputDir, 'missing-contacts.vcf');

    const writer = createWriteStream(tmpVcfPath);
    let nameCount = 0;
    let prevName = '';
    for (const contact of sortedMissingContacts) {
      let name = contact.name;
      if (makeNameUnique) {
        const count = duplicateNamesMap.get(String(name).toLowerCase());
        if (count > 1) {
          if (name.toLowerCase() === prevName.toLowerCase()) {
            nameCount++;
          } else {
            nameCount = 1;
            prevName = contact.name;
          }
          name = `${contact.name} (${nameCount})`;
          missingContacts.add(contact);
          // update in sortedMissingContacts
          contact.name = name;
        }
      }
      const vcard = new vCard();
      vcard.add('tel', contact.phone);
      vcard.add('fn', name);
      vcard.add('n', name);
      writer.write(vcard.toString() + '\n');
      this.stats.missing++;
    }
    writer.end(() => {
      const missingVcfPath = path.join(
        this.outputDir,
        `missing_numbers_total_${this.stats.missing}.vcf`
      );
      fs.rename(tmpVcfPath, missingVcfPath);
    });

    // After collecting contacts

    // Construct output filenames using outputDir and desired names
    const masterJsonPath = path.join(
      this.outputDir,
      `master_numbers_total_${this.stats.uniqueMasterContacts}.xlsx`
    );
    const compareJsonPath = path.join(
      this.outputDir,
      `compare_numbers_total_${this.stats.uniqueCompareContacts}.xlsx`
    );
    const missingXlsxPath = path.join(
      this.outputDir,
      `missing_numbers_total_${this.stats.missing}.xlsx`
    );
    const duplicateMasterPath = path.join(
      this.outputDir,
      `master_numbers_duplicate_${this.stats.uniqueMasterContacts}.xlsx`
    );
    const duplicateComparePath = path.join(
      this.outputDir,
      `compare_numbers_duplicate_${this.stats.uniqueCompareContacts}.xlsx`
    );
    const duplicateNameComparePath = path.join(
      this.outputDir,
      `name_duplicates.xlsx`
    );
    const singleOutputFilePath = path.join(this.outputDir, `output.xlsx`);

    const workbook = outputSingleXlsxFile ? XLSX.utils.book_new() : null;

    saveArrayAsXlsx(
      [...masterContacts.values()],
      masterJsonPath,
      'Master Contacts List',
      workbook,
      outputSingleXlsxFile
    );
    saveArrayAsXlsx(
      sortArrayByKey([...compareContacts.values()], 'name'),
      compareJsonPath,
      'Compare Contacts List',
      workbook,
      outputSingleXlsxFile
    );

    saveArrayAsXlsx(
      sortArrayByKey(
        mapToArrayOfObjects(duplicateMasterContacts),
        'value',
        true,
        true
      ),
      duplicateMasterPath,
      'Dupicate Master Contacts List',
      workbook,
      outputSingleXlsxFile
    );
    saveArrayAsXlsx(
      sortArrayByKey(
        mapToArrayOfObjects(duplicateCompareContacts),
        'value',
        true,
        true
      ),
      duplicateComparePath,
      'Dupicate Compare Contacts List',
      workbook,
      outputSingleXlsxFile
    );
    saveArrayAsXlsx(
      sortArrayByKey(
        mapToArrayOfObjects(duplicateNamesMap),
        'value',
        true,
        true
      ),
      duplicateNameComparePath,
      'Dupicate Name List',
      workbook,
      outputSingleXlsxFile
    );

    saveArrayAsXlsx(
      sortArrayByKey(sortedMissingContacts, 'name'),
      missingXlsxPath,
      'Missing Contacts',
      workbook,
      outputSingleXlsxFile
    );

    if (outputSingleXlsxFile) {
      saveArrayAsXlsx([], singleOutputFilePath, 'dummy', workbook, false);
    }

    return this.stats;
  }
}

function stringify(json) {
  return JSON.stringify(json, null, 2);
}
function mapToArrayOfObjects(map, keyName = 'key', valueName = 'value') {
  return Array.from(map.entries()).map(([key, value]) => {
    return {[keyName]: key, [valueName]: value};
  });
}

function sortArrayByKey(array, key, isNumber = false, isReverse = false) {
  const sortByNumberFn = (a, b) => {
    if (isReverse) {
      [a, b] = [b, a];
    }
    return a[key] - b[key];
  };
  const sortByStrFn = (a, b) => {
    if (isReverse) {
      [a, b] = [b, a];
    }
    const aVal = String(a[key]).toLowerCase();
    const bVal = String(b[key]).toLowerCase();
    return aVal.localeCompare(bVal);
  };

  return array.sort(isNumber ? sortByNumberFn : sortByStrFn);
}

async function main() {
  const processor = new ContactProcessor(
    './master_files',
    './compare_files',
    'output'
  );

  processor.on('error', console.error);
  processor.on('progress', console.log);

  try {
    const stats = await processor.process();
    console.log('Processing complete:');
    console.table({
      totalFiles: stats.totalFiles,
      masterFilesProcessed: stats.processed,
      totalMasterContacts: stats.totalMasterContacts,
      uniqueMasterContacts: stats.uniqueMasterContacts,
      totalCompareContacts: stats.totalCompareContacts,
      uniqueCompareContacts: stats.uniqueCompareContacts,
      missingContacts: stats.missing,
      errors: stats.errors,
    });
  } catch (error) {
    console.error('Fatal error:', error);
    process.exit(1);
  }
}
main();

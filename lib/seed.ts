import { sql } from '@vercel/postgres';
import fs from 'fs';
import csv from 'csv-parser';
import path from 'path';
import "dotenv/config";

// Updated parseDate function to interpret `MM/DD/YYYY`
function parseDate(dateString: string): string {
  const parts = dateString.split('/');
  if (parts.length === 3) {
    // CSV order is Month/Day/Year
    const month = parts[0].padStart(2, '0');
    const day = parts[1].padStart(2, '0');
    const year = parts[2];
    // Convert to YYYY-MM-DD for Postgres
    return `${year}-${month}-${day}`;
  }
  console.warn(`Could not parse date: ${dateString}`);
  throw Error();
}

export async function seed() {
  const createTable = await sql`
    CREATE TABLE IF NOT EXISTS unicorns (
      id SERIAL PRIMARY KEY,
      company VARCHAR(255) NOT NULL UNIQUE,
      valuation DECIMAL(10, 2) NOT NULL,
      date_joined DATE,
      country VARCHAR(255) NOT NULL,
      city VARCHAR(255) NOT NULL,
      industry VARCHAR(255) NOT NULL,
      select_investors TEXT NOT NULL
    );
  `;
  console.log(`Created "unicorns" table`);

  // 2. Read CSV file
  const results: any[] = [];
  const csvFilePath = path.join(process.cwd(), 'unicorns.csv');

  await new Promise((resolve, reject) => {
    fs.createReadStream(csvFilePath)
      .pipe(csv())
      .on('data', (data) => results.push(data))
      .on('end', resolve)
      .on('error', reject);
  });

  // 3. Insert CSV data into the table
  for (const row of results) {
    // If "Date Joined" is missing or empty
    if (!row['Date Joined'] || row['Date Joined'].trim() === '') {
      console.log(`Skipping row: date joined missing for "${row.Company}"`);
      continue; // Skip inserting this row
    }

    let formattedDate: string;
    try {
      formattedDate = parseDate(row['Date Joined']);
    } catch (e) {
      console.log(`Skipping row: invalid date for "${row.Company}" -> ${row['Date Joined']}`);
      continue;
    }

    await sql`
      INSERT INTO unicorns (
        company,
        valuation,
        date_joined,
        country,
        city,
        industry,
        select_investors
      )
      VALUES (
        ${row.Company},
        ${parseFloat(row['Valuation ($B)'].replace('$', '').replace(',', ''))},
        ${formattedDate},
        ${row.Country},
        ${row.City},
        ${row.Industry},
        ${row['Select Investors']}
      )
      ON CONFLICT (company) DO NOTHING;
    `;
  }

  console.log(`\nTotal CSV rows: ${results.length}`);

  return {
    createTable,
    unicorns: results,
  };
}

// Run the seed function
seed().catch(console.error);
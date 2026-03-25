#!/usr/bin/env node
/**
 * Export station metadata from Parse (Back4App) to a static JSON file.
 *
 * Usage:
 *   node export-stations.js
 *
 * Outputs: stations.json in the same directory
 */

const PARSE_APP_ID = 'zgqRRfY6Tr5ICfdPocRLZG8EXK59vfS5dpDM5bqr';
const PARSE_JS_KEY = '2KBoLKZWbEnRWK3lJ70ycxTTNRYKmC01pbdqbKXK';
const PARSE_SERVER = 'https://parseapi.back4app.com';

// Only states with avalanche forecasts
const AVALANCHE_STATES = ['AK','AZ','CA','CO','ID','MT','NV','NH','NM','OR','UT','WA','WY'];

const fs = require('fs');
const path = require('path');

const headers = {
    'X-Parse-Application-Id': PARSE_APP_ID,
    'X-Parse-Javascript-Key': PARSE_JS_KEY,
    'Content-Type': 'application/json'
};

async function parseQuery(className, where = {}, limit = 10000) {
    const results = [];
    let skip = 0;
    const batchSize = 1000; // Parse max per request

    while (true) {
        const params = new URLSearchParams({
            where: JSON.stringify(where),
            limit: Math.min(batchSize, limit - results.length),
            skip: skip
        });

        const resp = await fetch(`${PARSE_SERVER}/classes/${className}?${params}`, { headers });
        const data = await resp.json();

        if (data.error) {
            console.error(`Error querying ${className}:`, data.error);
            break;
        }

        if (!data.results || data.results.length === 0) break;
        results.push(...data.results);
        skip += data.results.length;

        if (data.results.length < batchSize || results.length >= limit) break;
        console.log(`  ... fetched ${results.length} ${className} records so far`);
    }

    return results;
}

async function exportSnotelStations() {
    console.log('Fetching SNOTEL stations...');
    const raw = await parseQuery('PFSnotelStation', {
        state: { $in: AVALANCHE_STATES },
        location: { $exists: true }
    });

    console.log(`  Found ${raw.length} SNOTEL stations`);

    // Also fetch SNOTEL metadata from USDA for names and elevations
    console.log('Fetching SNOTEL names from USDA API...');
    const nameMap = {};
    for (const state of AVALANCHE_STATES) {
        try {
            const url = `https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/stations?stationTriplets=*:${state}:SNTL&returnForecastPointMetadata=false&returnReservoirMetadata=false&returnStationElements=false&activeOnly=true`;
            const resp = await fetch(url);
            const data = await resp.json();
            if (Array.isArray(data)) {
                data.forEach(s => {
                    nameMap[s.stationTriplet] = { name: s.name, elevation: s.elevation };
                });
            }
            console.log(`  ${state}: ${Array.isArray(data) ? data.length : 0} stations from USDA`);
        } catch (err) {
            console.warn(`  Failed to fetch USDA data for ${state}:`, err.message);
        }
    }

    return raw.map(s => {
        const triplet = s.tripletID || '';
        const meta = nameMap[triplet] || {};
        return {
            type: 'snotel',
            tripletID: triplet,
            name: meta.name || triplet,
            lat: s.location?.latitude,
            lng: s.location?.longitude,
            elevation: meta.elevation || null,
            state: s.state || ''
        };
    }).filter(s => s.lat && s.lng);
}

async function exportMadisStations() {
    console.log('Fetching NWS/MADIS stations...');
    const raw = await parseQuery('PFNewMadisStation', {
        state: { $in: AVALANCHE_STATES },
        location: { $exists: true }
    });

    console.log(`  Found ${raw.length} MADIS stations`);

    return raw.map(s => ({
        type: 'madis',
        stationID: s.stationID || '',
        name: s.name || s.stationID || 'NWS Station',
        lat: s.location?.latitude,
        lng: s.location?.longitude,
        elevation: s.elevation || null,
        state: s.state || ''
    })).filter(s => s.lat && s.lng);
}

async function exportCoCoRaHSStations() {
    console.log('Fetching CoCoRaHS stations...');
    const raw = await parseQuery('PFCoCoRaHSStation', {
        state: { $in: AVALANCHE_STATES },
        location: { $exists: true }
    });

    console.log(`  Found ${raw.length} CoCoRaHS stations`);

    return raw.map(s => ({
        type: 'cocorahs',
        cocorahsID: s.coCoRaHS_ID || '',
        name: s.name || s.coCoRaHS_ID || 'CoCoRaHS Station',
        lat: s.location?.latitude,
        lng: s.location?.longitude,
        elevation: s.elevation || null,
        state: s.state || ''
    })).filter(s => s.lat && s.lng);
}

async function main() {
    console.log('=== Backcountry Ski Reporter - Station Export ===');
    console.log(`States: ${AVALANCHE_STATES.join(', ')}\n`);

    const [snotel, madis, cocorahs] = await Promise.all([
        exportSnotelStations(),
        exportMadisStations(),
        exportCoCoRaHSStations()
    ]);

    const output = {
        exportDate: new Date().toISOString(),
        states: AVALANCHE_STATES,
        snotel,
        madis,
        cocorahs
    };

    const outPath = path.join(__dirname, 'stations.json');
    fs.writeFileSync(outPath, JSON.stringify(output, null, 2));

    console.log(`\n=== Export Complete ===`);
    console.log(`SNOTEL:   ${snotel.length} stations`);
    console.log(`MADIS:    ${madis.length} stations`);
    console.log(`CoCoRaHS: ${cocorahs.length} stations`);
    console.log(`Total:    ${snotel.length + madis.length + cocorahs.length} stations`);
    console.log(`Output:   ${outPath}`);
}

main().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
});

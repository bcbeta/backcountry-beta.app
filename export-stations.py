#!/usr/bin/env python3
"""
Export station metadata from Parse (Back4App) to a static JSON file.

Usage:
    python3 export-stations.py

Outputs: stations.json in the same directory
"""

import json
import os
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime

PARSE_APP_ID = 'zgqRRfY6Tr5ICfdPocRLZG8EXK59vfS5dpDM5bqr'
PARSE_JS_KEY = '2KBoLKZWbEnRWK3lJ70ycxTTNRYKmC01pbdqbKXK'
PARSE_SERVER = 'https://parseapi.back4app.com'

# Only states with avalanche forecasts
AVALANCHE_STATES_ABBR = ['AK','AZ','CA','CO','ID','MT','NV','NH','NM','OR','UT','WA','WY']

# Parse stores full state names, USDA uses abbreviations
STATE_ABBR_TO_NAME = {
    'AK': 'Alaska', 'AZ': 'Arizona', 'CA': 'California', 'CO': 'Colorado',
    'ID': 'Idaho', 'MT': 'Montana', 'NV': 'Nevada', 'NH': 'New Hampshire',
    'NM': 'New Mexico', 'OR': 'Oregon', 'UT': 'Utah', 'WA': 'Washington',
    'WY': 'Wyoming'
}
AVALANCHE_STATE_NAMES = list(STATE_ABBR_TO_NAME.values())

def parse_query(class_name, where=None, limit=10000):
    """Query a Parse class with pagination."""
    results = []
    skip = 0
    batch_size = 1000

    while len(results) < limit:
        params = urllib.parse.urlencode({
            'where': json.dumps(where or {}),
            'limit': min(batch_size, limit - len(results)),
            'skip': skip
        })

        url = f'{PARSE_SERVER}/classes/{class_name}?{params}'
        req = urllib.request.Request(url, headers={
            'X-Parse-Application-Id': PARSE_APP_ID,
            'X-Parse-Javascript-Key': PARSE_JS_KEY,
            'Content-Type': 'application/json'
        })

        try:
            with urllib.request.urlopen(req) as resp:
                data = json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            print(f'  Error querying {class_name}: {e.code} {e.reason}')
            body = e.read().decode()
            print(f'  Response: {body[:200]}')
            break

        if 'error' in data:
            print(f'  Parse error: {data["error"]}')
            break

        batch = data.get('results', [])
        if not batch:
            break

        results.extend(batch)
        skip += len(batch)

        if len(batch) < batch_size:
            break

        print(f'  ... fetched {len(results)} {class_name} records so far')

    return results


def fetch_usda_snotel_metadata():
    """Fetch SNOTEL station names and elevations from the USDA API."""
    print('Fetching SNOTEL names from USDA API...')
    name_map = {}

    for state in AVALANCHE_STATES_ABBR:
        url = (
            f'https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/stations'
            f'?stationTriplets=*:{state}:SNTL'
            f'&returnForecastPointMetadata=false'
            f'&returnReservoirMetadata=false'
            f'&returnStationElements=false'
            f'&activeOnly=true'
        )
        req = urllib.request.Request(url)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
                if isinstance(data, list):
                    for s in data:
                        name_map[s.get('stationTriplet', '')] = {
                            'name': s.get('name', ''),
                            'elevation': s.get('elevation')
                        }
                    print(f'  {state}: {len(data)} stations from USDA')
        except Exception as e:
            print(f'  Failed USDA fetch for {state}: {e}')

    return name_map


def export_snotel_stations(usda_names):
    """Export SNOTEL stations from Parse, enriched with USDA names."""
    print('Fetching SNOTEL stations from Parse...')
    raw = parse_query('PFSnotelStation', {
        'state': {'$in': AVALANCHE_STATE_NAMES},
        'location': {'$exists': True}
    })
    print(f'  Found {len(raw)} SNOTEL stations in Parse')

    stations = []
    for s in raw:
        loc = s.get('location')
        if not loc:
            continue

        triplet = s.get('tripletID', '')
        meta = usda_names.get(triplet, {})

        stations.append({
            'type': 'snotel',
            'tripletID': triplet,
            'name': meta.get('name') or triplet,
            'lat': loc.get('latitude'),
            'lng': loc.get('longitude'),
            'elevation': meta.get('elevation'),
            'state': s.get('state', '')
        })

    return [st for st in stations if st['lat'] and st['lng']]


def export_madis_stations():
    """Export NWS/MADIS stations from Parse, querying per-state to avoid hitting limits."""
    print('Fetching NWS/MADIS stations from Parse (per-state)...')

    stations = []
    for state_name in AVALANCHE_STATE_NAMES:
        raw = parse_query('PFNewMadisStation', {
            'state': state_name,
            'location': {'$exists': True},
            'avalancheForecastZone': {'$exists': True, '$nin': ['No Zone', None]}
        })
        print(f'  {state_name}: {len(raw)} MADIS stations (in avalanche zones)')

        for s in raw:
            loc = s.get('location')
            if not loc:
                continue

            stations.append({
                'type': 'madis',
                'stationID': s.get('stationID', ''),
                'name': s.get('name') or s.get('stationID', 'NWS Station'),
                'lat': loc.get('latitude'),
                'lng': loc.get('longitude'),
                'elevation': s.get('elevation'),
                'state': s.get('state', ''),
                'zone': s.get('avalancheForecastZone', '')
            })

    valid = [st for st in stations if st['lat'] and st['lng']]
    print(f'  Total: {len(valid)} MADIS stations across all states')
    return valid


def export_cocorahs_stations():
    """Export CoCoRaHS stations from Parse."""
    print('Fetching CoCoRaHS stations from Parse...')
    raw = parse_query('PFCoCoRaHSStation', {
        'state': {'$in': AVALANCHE_STATE_NAMES},
        'location': {'$exists': True}
    })

    if not raw:
        print('  CoCoRaHS class not accessible or empty - skipping')
        return []

    print(f'  Found {len(raw)} CoCoRaHS stations in Parse')

    stations = []
    for s in raw:
        loc = s.get('location')
        if not loc:
            continue

        stations.append({
            'type': 'cocorahs',
            'cocorahsID': s.get('coCoRaHS_ID', ''),
            'name': s.get('name') or s.get('coCoRaHS_ID', 'CoCoRaHS Station'),
            'lat': loc.get('latitude'),
            'lng': loc.get('longitude'),
            'elevation': s.get('elevation'),
            'state': s.get('state', '')
        })

    return [st for st in stations if st['lat'] and st['lng']]


def main():
    print('=== Backcountry Ski Reporter - Station Export ===')
    print(f'States: {", ".join(AVALANCHE_STATES_ABBR)}\n')

    # Fetch USDA names first (needed for SNOTEL)
    usda_names = fetch_usda_snotel_metadata()

    print()
    snotel = export_snotel_stations(usda_names)
    madis = export_madis_stations()
    cocorahs = export_cocorahs_stations()

    output = {
        'exportDate': datetime.utcnow().isoformat() + 'Z',
        'states': AVALANCHE_STATES_ABBR,
        'snotel': snotel,
        'madis': madis,
        'cocorahs': cocorahs
    }

    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'stations.json')
    with open(out_path, 'w') as f:
        json.dump(output, f)

    # Also write a compact version for size check
    size_mb = os.path.getsize(out_path) / (1024 * 1024)

    print(f'\n=== Export Complete ===')
    print(f'SNOTEL:   {len(snotel)} stations')
    print(f'MADIS:    {len(madis)} stations')
    print(f'CoCoRaHS: {len(cocorahs)} stations')
    print(f'Total:    {len(snotel) + len(madis) + len(cocorahs)} stations')
    print(f'File:     {out_path}')
    print(f'Size:     {size_mb:.2f} MB')


if __name__ == '__main__':
    main()

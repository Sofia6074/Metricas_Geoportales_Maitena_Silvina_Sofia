import polars as pl;

# Abro el archivo
with open('access.log', 'r') as file:
    log_data = file.readlines()

# Lo leo linea por linea y voy guardando la info
log_entries = []
for line in log_data:
    parts = line.split(' ')
    try:
        ip = parts[0]
        timestamp = ' '.join(parts[3:5]).strip('[]')
        request_method = parts[5].strip('"') if parts[5] != '"-"' else None
        url = parts[6] if len(parts) > 6 else None
        http_version = parts[7].strip('"') if len(parts) > 7 else None
        status_code = int(parts[8]) if len(parts) > 8 and parts[8].isdigit() else None
        size = int(parts[9]) if len(parts) > 9 and parts[9].isdigit() else None
        user_agent = ' '.join(parts[11:]).strip('"') if len(parts) > 11 else None

        log_entries.append((ip, timestamp, request_method, url, http_version, status_code, size, user_agent))

    except (IndexError, ValueError) as e:
        print(f"Error procesando la línea: {line.strip()} - Error: {e}")
        # Pongo null en caso de que de error
        log_entries.append((ip, timestamp, None, None, None, None, None, None))

# Dataframe final
df = pl.DataFrame(
    log_entries,
    schema=[
        ("ip", pl.Utf8),
        ("timestamp", pl.Utf8),
        ("request_method", pl.Utf8),
        ("url", pl.Utf8),
        ("http_version", pl.Utf8),
        ("status_code", pl.Int32),
        ("size", pl.Int32),
        ("user_agent", pl.Utf8),
    ]
)

print(df)
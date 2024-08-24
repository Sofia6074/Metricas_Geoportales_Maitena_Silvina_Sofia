import csv

log_file_path = '/Users/admin/Documents/TesisArchivo/access.log'
csv_file_path = '/Users/admin/Documents/TesisArchivo/archivo.csv'

with open(log_file_path, 'r') as file:
    log_data = file.readlines()

# Encabezados
headers = ["ip", "timestamp", "request_method", "url", "http_version", "status_code", "size", "user_agent"]

with open(csv_file_path, 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(headers)

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

            csvwriter.writerow([ip, timestamp, request_method, url, http_version, status_code, size, user_agent])

        except (IndexError, ValueError) as e:
            print(f"Error procesando la línea: {line.strip()} - Error: {e}")
            csvwriter.writerow([ip, timestamp, None, None, None, None, None, None])

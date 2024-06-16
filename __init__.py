import os
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import re

def parse_log_line(line):
    stripped = line.strip().split('"')
    date = stripped[0].strip()[1:-1]
    pattern = r'"(.*?)"'
    matches = re.findall(pattern, line)
    url = matches[1]
    code = matches[2]
    
    return date, url, code

def aggregate_data(log_file_path):
    requests_per_hour = defaultdict(int)
    requested_resources = Counter()
    response_codes_distribution = Counter()
    error_timestamps = []

    with open(log_file_path, 'r') as file:
        for line in file:
            date, url, code = parse_log_line(line)
            datetime_obj = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S')
            hour_timestamp = datetime_obj.strftime('%Y-%m-%d %H')
            requests_per_hour[hour_timestamp] += 1
            requested_resources[url] += 1
            response_codes_distribution[code] += 1
            
            if code.isdigit() and 500 <= int(code) <= 599:
                error_timestamps.append(datetime_obj)
    
    return requests_per_hour, requested_resources, response_codes_distribution, error_timestamps

def detect_anomalies(requests_per_hour, error_timestamps):
    anomalies = []
    
    request_spike_threshold = 10
    
    # 5 errors within 5 minutes
    error_rate_threshold = 5
    time_window_minutes = 5

    for hour, count in requests_per_hour.items():
        if count > request_spike_threshold:
            anomalies.append(f"{hour}: {count} requests")
    
    error_timestamps.sort()
    if error_timestamps:
        window_start = error_timestamps[0]
        window_end = window_start + timedelta(minutes=time_window_minutes)
        error_count = 0

        for timestamp in error_timestamps:
            if timestamp <= window_end:
                error_count += 1
            else:
                if error_count > error_rate_threshold:
                    anomalies.append(f"High error rate detected between {window_start} and {window_end}: {error_count} errors")
                window_start = timestamp
                window_end = window_start + timedelta(minutes=time_window_minutes)
                error_count = 1
        
        # Check the last window
        if error_count > error_rate_threshold:
            anomalies.append(f"High error rate detected between {window_start} and {window_end}: {error_count} errors")

    return anomalies

def print_report(requests_per_hour, requested_resources, response_codes_distribution, anomalies):
    requests_per_hour_str = '\n'.join([f'{hour}: {count}' for hour, count in sorted(requests_per_hour.items())])
    most_requested_resources_str = '\n'.join([f'{resource}: {count}' for resource, count in requested_resources.most_common()])
    response_code_distribution_str = '\n'.join([f'{code}: {count}' for code, count in response_codes_distribution.items()])
    detected_anomalies_str = '\n'.join(anomalies)

    report_template = f"""
Web Server Traffic Report

Requests Per Hour:
{requests_per_hour_str}

Most Requested Resources:
{most_requested_resources_str}

Response Code Distribution:
{response_code_distribution_str}

Detected Anomalies:
{detected_anomalies_str}
    """
    print(report_template)

def main():
    log_file_path = os.path.join('logs', 'log2.txt')
    requests_per_hour, requested_resources, response_codes_distribution, error_timestamps = aggregate_data(log_file_path)
    anomalies = detect_anomalies(requests_per_hour, error_timestamps)
    print_report(requests_per_hour, requested_resources, response_codes_distribution, anomalies)

if __name__ == "__main__":
    main()

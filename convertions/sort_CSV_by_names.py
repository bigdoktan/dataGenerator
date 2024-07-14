import csv

def sort_csv_by_name(csv_file_path):
    # Read the CSV file into a list of dictionaries
    with open(csv_file_path, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        data = [row for row in reader]

    # Sort the data by the 'name' column
    sorted_data = sorted(data, key=lambda row: row['name'])

    # Write the sorted data back to a new CSV file
    sorted_csv_file_path = csv_file_path[:-4] + '-sorted.csv'  # Create a new filename
    with open(sorted_csv_file_path, 'w', newline='') as csvfile:
        fieldnames = data[0].keys() if data else []  # Get the fieldnames from the first row (headers)
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sorted_data)

    return sorted_csv_file_path

if __name__ == "__main__":
    input_csv_file = "../data generation script/pii.csv"
    sorted_csv_file = sort_csv_by_name(input_csv_file)
    print(f"CSV file sorted and saved as: {sorted_csv_file}")

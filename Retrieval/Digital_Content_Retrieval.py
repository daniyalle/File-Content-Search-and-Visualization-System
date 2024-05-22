import os
import mysql.connector
from mysql.connector import errorcode
from bs4 import BeautifulSoup
import chardet
import logging
import re
import csv
import matplotlib.pyplot as plt

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to create a table based on the provided SQL query
def create_table(connection, query, table_name):
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        logging.info(f"Table '{table_name}' created successfully")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            logging.warning(f"Table '{table_name}' already exists.")
        else:
            logging.error(f"Error creating table '{table_name}': {err}")
    finally:
        cursor.close()

# Function to create the 'all_files' table if it doesn't exist
def create_files_table(connection):
    query = """
        CREATE TABLE IF NOT EXISTS all_files (
            id INT AUTO_INCREMENT PRIMARY KEY,
            file_name VARCHAR(255),
            full_path VARCHAR(255),
            file_type VARCHAR(55),
            file_size INT,
            content LONGTEXT
        )
    """
    create_table(connection, query, 'all_files')

# Function to create the 'search_results' table if it doesn't exist
def create_search_results_table(connection):
    query = """
        CREATE TABLE IF NOT EXISTS search_results (
            id INT AUTO_INCREMENT PRIMARY KEY,
            file_name VARCHAR(255),
            full_path VARCHAR(255),
            file_type VARCHAR(55),
            file_size INT,
            content LONGTEXT,
            occurrence_num INT
        )
    """
    create_table(connection, query, 'search_results')

# Function to extract text from an HTML file using BeautifulSoup and chardet for encoding detection
def extract_text_from_html(file_path):
    try:
        with open(file_path, 'rb') as f:
            raw_data = f.read()
            result = chardet.detect(raw_data)
            encoding = result['encoding']
            if encoding is None:
                logging.warning(f"Could not detect encoding for {file_path}. Trying common encodings.")
                encodings_to_try = ['utf-8', 'iso-8859-1', 'windows-1252']
                for enc in encodings_to_try:
                    try:
                        html_content = raw_data.decode(enc, errors='ignore')
                        soup = BeautifulSoup(html_content, 'lxml')
                        text = soup.get_text()
                        logging.info(f"Text extracted using {enc} encoding.")
                        return text
                    except Exception as e:
                        logging.error(f"Error decoding with {enc}: {e}")
                return None
            else:
                html_content = raw_data.decode(encoding, errors='ignore')
                soup = BeautifulSoup(html_content, 'lxml')
                text = soup.get_text()
                logging.info(f"Text extracted using detected encoding ({encoding}).")
                return text
    except Exception as e:
        logging.error(f"Error extracting text from HTML file {file_path}: {e}")
        return None

# Function to insert files from a directory into the 'all_files' table
def insert_files_into_table(directory, connection, file_types):
    try:
        cursor = connection.cursor()
        # Walk through the directory
        for root, _, files in os.walk(directory):
            for file in files:
                file_path = os.path.join(root, file)
                file_name, file_extension = os.path.splitext(file)

                if file_extension.lower() in file_types:
                    logging.info(f"Skipping file due to filter: {file_path}")
                    continue

                try:
                    file_size = os.path.getsize(file_path)
                    if file_extension.lower() == '.html':
                        content = extract_text_from_html(file_path)
                    else:
                        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                            content = f.read()
                except Exception as e:
                    logging.error(f"Could not read file {file_path}: {e}")
                    content = None

                if content is not None:
                    try:
                        # Insert file data into all_files table
                        cursor.execute("""
                            INSERT INTO all_files (file_name, full_path, file_type, file_size, content)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (file_name, file_path, file_extension, file_size, content))
                        logging.info(f"Inserted file: {file_path}")
                    except mysql.connector.Error as err:
                        logging.error(f"Error inserting file {file_path}: {err}")
                else:
                    logging.warning(f"Content is None for file: {file_path}")

        connection.commit()
        logging.info("Files inserted into 'all_files' table successfully")
    except mysql.connector.Error as err:
        logging.error(f"Error inserting files: {err}")
    finally:
        cursor.close()

# Function to search for a string in the 'all_files' table and save results in 'search_results' table and a CSV file
def search_files(connection, search_string):
    try:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM search_results")

        search_query = """
            SELECT id, file_name, full_path, file_type, file_size, content
            FROM all_files
            WHERE file_name LIKE %s
            OR full_path LIKE %s
            OR file_type LIKE %s
            OR content LIKE %s
        """
        cursor.execute(search_query, ('%' + search_string + '%',) * 4)

        results = cursor.fetchall()
        if results:
            total_occurrences = 0
            file_occurrences = {}  # Dictionary to store occurrences per file
            csv_file_path = "search_results.csv"
            with open(csv_file_path, mode='w', newline='', encoding='utf-8') as csv_file:
                csv_writer = csv.writer(csv_file)
                csv_writer.writerow(['file_name', 'full_path', 'file_type', 'file_size', 'occurrence_num'])

                for row in results:
                    file_id, file_name, full_path, file_type, file_size, content = row
                    occurrence_num = len(re.findall(re.escape(search_string), content, re.IGNORECASE)) if content else 0
                    occurrence_num += 1 if search_string.lower() in file_name.lower() else 0  # Check file_name
                    total_occurrences += occurrence_num
                    file_occurrences[file_name] = occurrence_num

                    if occurrence_num > 0:
                        cursor.execute("""
                            INSERT INTO search_results (file_name, full_path, file_type, file_size, content, occurrence_num)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (file_name, full_path, file_type, file_size, content, occurrence_num))

                        csv_writer.writerow([file_name, full_path, file_type, file_size, occurrence_num])

            connection.commit()
            logging.info("Search results saved in 'search_results' table and exported to CSV")

            # Summary Statistics
            logging.info(f"Total occurrences of '{search_string}': {total_occurrences}")
            print(f"Summary Statistics:\n- Total occurrences of '{search_string}': {total_occurrences}")

            # Plot occurrences per file
            non_zero_occurrences = {file_name: occurrences for file_name, occurrences in file_occurrences.items() if
                                    occurrences > 0}
            sorted_files = sorted(non_zero_occurrences.items(), key=lambda x: x[1])
            file_names = [file[0] for file in sorted_files]
            occurrences_per_file = [file[1] for file in sorted_files]

            plt.figure(figsize=(10, 6))
            bar_height = 0.3  # Bar thickness
            spacing = 1.3  # Additional space between bars
            y_positions = [i * (bar_height + spacing) for i in range(len(file_names))]
            plt.barh(y_positions, occurrences_per_file, color='skyblue', height=bar_height)
            plt.yticks(y_positions, file_names)
            plt.xlabel('Occurrences')
            plt.ylabel('File Name')
            plt.title('Occurrences of Search String per File')
            plt.tight_layout()
            plt.show()

        else:
            logging.info("No files found with the specified search string.")
    except mysql.connector.Error as err:
        logging.error(f"Error searching files: {err}")
    finally:
        cursor.close()


# Main function to set up database connection, create tables, insert files, and perform search
def main():
    host = input("Enter the database host: ")
    user = input("Enter the database user: ")
    password = input("Enter the database password: ")
    database = input("Enter the database name: ")

    search_directory = input("Enter the directory to search: ")
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        if connection.is_connected():
            logging.info("Connected to MySQL database")

        create_files_table(connection)
        create_search_results_table(connection)

        file_types_to_exclude = input("Enter file types to exclude (comma separated, e.g., '.php,.txt'): ").split(',')
        insert_files_into_table(search_directory, connection, file_types_to_exclude)

        search_string = input("Enter the search string: ")
        search_files(connection, search_string)

    except mysql.connector.Error as err:
        logging.error(f"Error connecting to MySQL database: {err}")
    finally:
        if connection.is_connected():
            connection.close()
            logging.info("MySQL connection is closed")

if __name__ == "__main__":
    main()

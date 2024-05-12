from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import re
import unicodedata
import os

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Function to extract links from a webpage
def extract_links(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    links = [link.get('href') for link in soup.find_all('a')]
    return links

# Function to extract titles and descriptions from articles
def extract_article_info(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    titles = [title.text for title in soup.find_all('h2')]
    descriptions = [desc.text for desc in soup.find_all('p')]
    return titles, descriptions

# Function to clean text
def clean_text(text):
    # Remove HTML tags
    text = BeautifulSoup(text, 'html.parser').get_text()

    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text)

    # Remove non-alphanumeric characters
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)

    # Convert to lowercase
    text = text.lower()

    # Normalize Unicode characters
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8', 'ignore')

    return text

# Function to write data to a file
def write_to_file(data, filepath):
    with open(filepath, 'a', encoding='utf-8') as file:
        for item in data:
            file.write("%s\n" % item)

# DAG definition
dag = DAG(
    'web_scraping_and_preprocessing',
    default_args=default_args,
    description='A DAG to scrape data from websites and preprocess it',
    schedule_interval=None,
)

# Task to extract links from Dawn and BBC
def task_extract_links():
    dawn_links = extract_links('https://www.dawn.com')
    bbc_links = extract_links('https://www.bbc.com')
    return dawn_links, bbc_links

# Task to extract titles and descriptions from BBC and Dawn
def task_extract_article_info():
    bbc_titles, bbc_descriptions = extract_article_info('https://www.bbc.com')
    dawn_titles, dawn_descriptions = extract_article_info('https://www.dawn.com')
    return bbc_titles, bbc_descriptions, dawn_titles, dawn_descriptions

# Task to preprocess data
def task_preprocess_data(dawn_links, bbc_links, bbc_titles, bbc_descriptions, dawn_titles, dawn_descriptions):
    cleaned_dawn_links = [clean_text(link) for link in dawn_links]
    cleaned_bbc_links = [clean_text(link) for link in bbc_links]
    cleaned_dawn_titles = [clean_text(title) for title in dawn_titles]
    cleaned_dawn_descriptions = [clean_text(desc) for desc in dawn_descriptions]
    cleaned_bbc_titles = [clean_text(title) for title in bbc_titles]
    cleaned_bbc_descriptions = [clean_text(desc) for desc in bbc_descriptions]
    return cleaned_dawn_links, cleaned_bbc_links, cleaned_bbc_titles, cleaned_bbc_descriptions, cleaned_dawn_titles, cleaned_dawn_descriptions

# Task to write data to files
def task_write_to_files(cleaned_dawn_links, cleaned_bbc_links, cleaned_bbc_titles, cleaned_bbc_descriptions, cleaned_dawn_titles, cleaned_dawn_descriptions):
    bbc_links_file = os.path.join('data', 'bbc_links.txt')
    dawn_links_file = os.path.join('data', 'dawn_links.txt')
    bbc_titles_file = os.path.join('data', 'bbc_titles.txt')
    dawn_titles_file = os.path.join('data', 'dawn_titles.txt')
    bbc_descriptions_file = os.path.join('data', 'bbc_descriptions.txt')
    dawn_descriptions_file = os.path.join('data', 'dawn_descriptions.txt')

    write_to_file(cleaned_bbc_links, bbc_links_file)
    write_to_file(cleaned_dawn_links, dawn_links_file)
    write_to_file(cleaned_bbc_titles, bbc_titles_file)
    write_to_file(cleaned_dawn_titles, dawn_titles_file)
    write_to_file(cleaned_bbc_descriptions, bbc_descriptions_file)
    write_to_file(cleaned_dawn_descriptions, dawn_descriptions_file)

# Define tasks
extract_links_task = PythonOperator(
    task_id='extract_links',
    python_callable=task_extract_links,
    dag=dag,
)

extract_article_info_task = PythonOperator(
    task_id='extract_article_info',
    python_callable=task_extract_article_info,
    dag=dag,
)

preprocess_data_task = PythonOperator(
    task_id='preprocess_data',
    python_callable=task_preprocess_data,
    dag=dag,
)

write_to_files_task = PythonOperator(
    task_id='write_to_files',
    python_callable=task_write_to_files,
    dag=dag,
)

# Define task dependencies
extract_links_task >> extract_article_info_task >> preprocess_data_task >> write_to_files_task

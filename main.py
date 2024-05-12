import requests
from bs4 import BeautifulSoup
import re
import unicodedata
import os

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

# Extracting links from dawn and bbc
dawn_links = extract_links('https://www.dawn.com')
bbc_links = extract_links('https://www.bbc.com')

# Extracting titles and descriptions from BBC and dawn
bbc_titles, bbc_descriptions = extract_article_info('https://www.bbc.com')
dawn_titles, dawn_descriptions = extract_article_info('https://www.dawn.com')

# print("Dawn Links:", dawn_links)
# print()

# print("Dawn Titles:", dawn_titles)
# print()

# print("Dawn Descriptions:", dawn_descriptions)
# print()


# print("BBC Links:", bbc_links)
# print()

# print("BBC Titles:", bbc_titles)
# print()

# print("BBC Descriptions:", bbc_descriptions)





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

# Preprocess Dawn Links
cleaned_dawn_links = [clean_text(link) for link in dawn_links]

# Preprocess BBC Links
cleaned_bbc_links = [clean_text(link) for link in bbc_links]

# Preprocess Dawn Titles
cleaned_dawn_titles = [clean_text(title) for title in dawn_titles]

# Preprocess Dawn Descriptions
cleaned_dawn_descriptions = [clean_text(desc) for desc in dawn_descriptions]

# Preprocess BBC Titles
cleaned_bbc_titles = [clean_text(title) for title in bbc_titles]

# Preprocess BBC Descriptions
cleaned_bbc_descriptions = [clean_text(desc) for desc in bbc_descriptions]

print("Cleaned Dawn Links:", cleaned_dawn_links)
print()
print("Cleaned BBC Links:", cleaned_bbc_links)
print()
print("Cleaned Dawn Titles:", cleaned_dawn_titles)
print()
print("Cleaned Dawn Descriptions:", cleaned_dawn_descriptions)
print()
print("Cleaned BBC Titles:", cleaned_bbc_titles)
print()
print("Cleaned BBC Descriptions:", cleaned_bbc_descriptions)



# Function to write data to a file
def write_to_file(data, filepath):
    with open(filepath, 'a', encoding='utf-8') as file:
        for item in data:
            file.write("%s\n" % item)

# Define file paths
bbc_links_file = os.path.join('data', 'bbc_links.txt')
dawn_links_file = os.path.join('data', 'dawn_links.txt')
bbc_titles_file = os.path.join('data', 'bbc_titles.txt')
dawn_titles_file = os.path.join('data', 'dawn_titles.txt')
bbc_descriptions_file = os.path.join('data', 'bbc_descriptions.txt')
dawn_descriptions_file = os.path.join('data', 'dawn_descriptions.txt')

# Write data to files
write_to_file(cleaned_bbc_links, bbc_links_file)
write_to_file(cleaned_dawn_links, dawn_links_file)
write_to_file(cleaned_bbc_titles, bbc_titles_file)
write_to_file(cleaned_dawn_titles, dawn_titles_file)
write_to_file(cleaned_bbc_descriptions, bbc_descriptions_file)
write_to_file(cleaned_dawn_descriptions, dawn_descriptions_file)

# Stage files with DVC
os.system(f'dvc add {bbc_links_file}')
os.system(f'dvc add {dawn_links_file}')
os.system(f'dvc add {bbc_titles_file}')
os.system(f'dvc add {dawn_titles_file}')
os.system(f'dvc add {bbc_descriptions_file}')
os.system(f'dvc add {dawn_descriptions_file}')
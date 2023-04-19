import xml.etree.ElementTree as et # to parse the given xml
import requests # to download the zip files mentioned
import os


def download_docs(filename : str) -> None:
    tree = et.parse(filename)

    document =tree.getroot()
    doc_tags = document.find('result')
    num = 1
    for doc in doc_tags:
        
        download_link_tag = doc.find('str[@name="download_link"]')
        download_link = download_link_tag.text
        
        suffix = "st" if num == 1 else "nd" if num == 2 else "rd" if num == 3 else "th"
        
        print(f'Starting downloading {num}{suffix} download link')
        
        r = requests.get(download_link, allow_redirects=True)
        downloaded_file_name = download_link.split('/')[-1]
        
        if not os.path.exists('supporting_files'):
            os.mkdir('supporting_files')

        with open (f'supporting_files\{downloaded_file_name}', 'wb') as f:
            f.write(r.content)
        
        print(f'Finishing downloading and writing {num}{suffix} download link')

        num += 1
    print('Finished Parsing and Writing the data')


if __name__ == '__main__':
    download_docs('downloadedxml.xml')
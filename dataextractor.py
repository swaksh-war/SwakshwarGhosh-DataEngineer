import os
from zipfile import ZipFile
import xml.etree.ElementTree as et
import pandas as pd
import boto3
from zipdownloader import download_docs

class DataExtractor:
    '''
    This Class uses two methods:-
    1. extract_zip -> This function takes a directory as an input and extracts the xml file inside it and 
    stores the file location inside the class to be used in later process

    2. extract_from_xml -> This Function will extract the required attributes from the XMLs That we extracted in the extract_zip function and store them in a CSV as output 
    '''
    def __init__(self):
        self.xml_file_names = []
        self.csv_file_names = []

    def extract_zip(self, containing_folder_or_file : str) -> None:
        '''
        This function takes a directory as an input and extracts the xml file inside it and 
        stores the file location inside the class to be used in later process.
        '''

        #Checking if path exists or not
        if os.path.exists(containing_folder_or_file):
            #Checking whether its a directory
            if os.path.isdir(containing_folder_or_file):
                for file in os.listdir(containing_folder_or_file):
                    with  ZipFile(f'{containing_folder_or_file}\{file}', 'r') as f:
                        os.mkdir(file)
                        f.extractall(file)
                        xml_file_name = str(file.split('.')[0])
                        self.xml_file_names.append(f'{file}\{xml_file_name}.xml')
            #else its a file 
            else:
                with ZipFile(containing_folder_or_file) as f:
                    os.mkdir(containing_folder_or_file)
                    f.extractall(file)
                    xml_file_name = str(file.split('.')[0])
                    self.xml_file_names.append(f'{file}\{xml_file_name}.xml')
        
        else:
            print("Path does\'t exist")

    def extract_from_xml(self) -> None:
        '''
        This Function will extract the required attributes from the XMLs That we extracted in the extract_zip function and store them in a CSV as output.
        '''


        for file in self.xml_file_names:
            tree = et.parse(file)
            document = tree.getroot()
            #extracting all the id elements from the xml.
            all_fininsterm_id_elem = document.findall(".//{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}Id")
            #extracting the ids from the above elements.
            all_fininsterm_id = [i.text for i in all_fininsterm_id_elem]

            #extracting all the issr elements from the xml.
            all_issr_elem = document.findall(".//{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}Issr")
            #extracting the issr from the above elements.
            all_issr = [i.text for i in all_issr_elem]

            #extracting all the fullname elements from the xml.
            all_fullnm_elem = document.findall(".//{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}FullNm")
            #extracting the fullname from the above elements.
            all_fullnm = [i.text for i in all_fullnm_elem]

            #extracting all the classfnttp elements from the xml.
            all_classfnttp_elem = document.findall(".//{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}ClssfctnTp")
            #extracting all the classfnttp elements from the above element.
            all_classfnttp = [i.text for i in all_classfnttp_elem]

            #extracting all the ntnlccy elements from the xml.
            all_ntnlccy_elem = document.findall(".//{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}NtnlCcy")
            #extracting all the ntnlccy elements from the above element.
            all_ntnlccy = [i.text for i in all_ntnlccy_elem]

            #extracting all the CmmdtyDerivInd elements from the xml.
            all_cmmdtderivind_elem = document.findall(".//{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}NtnlCcy")
            #extracting all the cmmdtyderivind elements from the above element
            all_cmmdtderivind = [i.text for i in all_cmmdtderivind_elem]

            #extracting the actual FinInstrmGnlAttrbts ids from all the ids.
            final_id = [all_fininsterm_id[i] for i in range(0, len(all_fininsterm_id), 2)]
            
            #creating the data to stored in the dataframe.
            data = {'FinInstrmGnlAttrbts.Id':final_id,
                    'FinInstrmGnlAttrbts.FullNm':all_fullnm,
                    'FinInstrmGnlAttrbts.ClssfctnTp':all_classfnttp,
                    'FinInstrmGnlAttrbts.CmmdtyDerivInd':all_cmmdtderivind,
                    'FinInstrmGnlAttrbts.NtnlCcy':all_ntnlccy,
                    'Issr':all_issr
                    }
            #creating the dataframe the data.
            df = pd.DataFrame(data)
            filename = file.split('\\')[-1]
            #creating the csvs.
            df.to_csv(f'{filename}.csv', sep=',')
            self.csv_file_names.append(f'{filename}.csv')
    
    def upload_to_s3(self, aws_access_key : str, aws_secret_access_key : str, bucket_name : str) -> None:
        '''
        This Function will upload the files accessing from the csv_file_names list in the class and upload it to the s3 bucket.
        parameters -> aws_access_key - Your AWS access key.
        aws_secret_access_key - Your AWS Secret access key.
        bucket_name - Your Buncket name.
        '''
        access_key = aws_access_key
        secret_access_key = aws_secret_access_key
        s3_bucket_name = bucket_name

        s3 = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key
            )
        for file in self.csv_file_names:
            s3.upload_file(file, s3_bucket_name, file)
        print(f"All file uploaded to {bucket_name} successfully")
if __name__ == '__main__':
    download_docs('downloadedxml.xml')
    de = DataExtractor()
    de.extract_zip('supporting_files')
    de.extract_from_xml()
    de.upload_to_s3('your aws access key','your aws secret access key','your bucket name')


import datetime
import json
import random
import sqlite3
from time import sleep

import pandas as pd
import requests
from bs4 import BeautifulSoup


index = 'Domain Name'

def extract_domain_bl_as_df(td_element):
    main_link_text = td_element.find('a').get_text(strip=True)
    main_link_href = td_element.find('a')['href']
    base = 'https://expireddomains.net'
    # Extract the additional links and create a dictionary with link text as keys and hrefs as values
    link_data = {li.a.get_text(strip=True): f"{base}{li.a['href']}" for li in td_element.select('ul.kmenucontent li') if li.a}

    # Add the main link to the dictionary
    link_data['Main Link'] = main_link_href
    link_data['Main Link Text'] = main_link_text
    return link_data


def extract_domain_data_as_df(td_element):
    # Extract the domain name
    domain_name = td_element.find('a', class_='namelinks').get_text(strip=True)
    base = 'https://expireddomains.net'
    # Extract the links
    link_data = {li.a.get_text(strip=True): f"{base}{li.a['href']}" for li in td_element.select('ul.kmenucontent li')}

    # Add the domain name to the dictionary
    link_data['Domain Name'] = domain_name

    return link_data
    # Creating a DataFrame
    df = pd.DataFrame([link_data])

    return df


def get_data(start):
    url = f'https://member.expireddomains.net/domains/expiredcom/?start={start}&o=acpc&r=d#listing'
    headers = {
        'authority': 'member.expireddomains.net',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'cookie': 'ExpiredDomainssessid=V5DofUxYdIEPXn7unLtjdtNI1t0Qzf6SwdM19NJzuG2gFVH%2Cl1D4B1Pz%2Cs728tQGf0KNwio72dieln6m6AettJPCSfKNJJZnnmT%2Cs0KVuxi4emol0ZAOr8fbp2nEsoWr; reme=ahmedyusef9%3A%7C%3AlgbxofPMzpFPcURPvocVSTxDO3IHwSkCMQIKhVf5',

    }

    response = requests.get(url, headers=headers)
    return response


def parse_table(response) -> list:
    soup = BeautifulSoup(response.content, 'html.parser')

    table = soup.find('table')

    # Initialize an empty list to hold all rows of the table
    table_data = []
    classes = []
    # Check if a table is found
    if table:
        # Iterate over table rows
        results = []
        for row in table.find_all('tr'):
            # Extract each cell from the row
            cells = row.find_all(['td', 'th'])
            # Extract text from each cell
            dd = {}
            for tag in cells:

                if tag and 'field_relatedlinks' in tag.get('class', []):
                    pass
                elif tag and 'field_bl' in tag.get('class', []):
                    dd.update(extract_domain_bl_as_df(tag))
                elif tag and 'field_domain' in tag.get('class', []):
                    dd.update(extract_domain_data_as_df(tag))
                else:
                    class_name = tag.get('class', [])
                    if class_name:
                        dd[class_name[0]] = tag.get_text(strip=True)
            results.append(dd)
        return results
        #     cell_texts = [cell.get_text(strip=True) for cell in cells]
        #     # Append the cell text to the table data
        #     table_data.append(cell_texts)
        #
        # # Create a DataFrame
        # # Assuming the first row of the data is the header
        # df = pd.DataFrame(table_data[1:], columns=table_data[0])
        # print(df)


def connection_db():
    return sqlite3.connect(f'local_expired_{str(datetime.datetime.now())}.db')

def create_table(conn):
    conn.execute('CREATE TABLE IF NOT EXISTS domains (key TEXT PRIMARY KEY, data JSON)')


def insert_to_db(conn, df):
    for key, data in zip(df.index, df['json']):
        conn.execute('INSERT INTO domains (key, data) VALUES (?, ?)', (key, data))
    conn.commit()


if __name__ == '__main__':
    conn = connection_db()
    create_table(conn)
    ddd = []
    for ss in list(range(0, 10000, 25)):
        try:
            response = get_data(ss)
            result = parse_table(response)
            df = pd.DataFrame(data=result[1:])
            df.set_index(index, inplace=True)
            df['json'] = df.apply(lambda row: json.dumps(row.to_dict()), axis=1)
            insert_to_db(conn, df)
            print(f"fetched {ss + 25} ...")
            sleep(random.choice([0.5, 1, 2, 0.7]))
        except Exception as ee:
            print(f"{str(ee)}")
    conn.close()
    df = pd.DataFrame(data=ddd)
    df.to_csv('table.csv')
    # soup = BeautifulSoup(response.content, 'html.parser')
    #
    # table = soup.find('table')
    #
    # # Initialize an empty list to hold all rows of the table
    # table_data = []
    # classes = []
    # # Check if a table is found
    # if table:
    #     # Iterate over table rows
    #     results = []
    #     for row in table.find_all('tr'):
    #         # Extract each cell from the row
    #         cells = row.find_all(['td', 'th'])
    #         # Extract text from each cell
    #         dd = {}
    #         for tag in cells:
    #
    #             if tag and 'field_relatedlinks' in tag.get('class', []):
    #                 pass
    #             elif tag and 'field_bl' in tag.get('class', []):
    #                 dd.update(extract_domain_bl_as_df(tag))
    #             elif tag and 'field_domain' in tag.get('class', []):
    #                 dd.update(extract_domain_data_as_df(tag))
    #             else:
    #                 class_name = tag.get('class', [])
    #                 if class_name:
    #                     dd[class_name[0]] = tag.get_text(strip=True)
    #         results.append(dd)
    #
    #         cell_texts = [cell.get_text(strip=True) for cell in cells]
    #         # Append the cell text to the table data
    #         table_data.append(cell_texts)
    #
    #     # Create a DataFrame
    #     # Assuming the first row of the data is the header
    #     df = pd.DataFrame(table_data[1:], columns=table_data[0])
    #     print(df)
    #
    # print(response.text)

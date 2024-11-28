import requests
import pandas as pd
import datetime as dt


class OfferCreateDataCollector():
    def __init__(self):
        self.base_url = 'https://api.xrpscan.com/api/v1'
        self.headers = {
            'Authorization': '',
            'User-Agent': 'OfferCreateCollection/0.0.2',
        }


    def get_data(self, ledger_number):
        endpoint = f'/ledgers/{ledger_number}/transactions'
        url = self.base_url + endpoint

        ledger_entries = []
        row_base = {'Ledger_Number': ledger_number}

        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            response = response.json()
    
            for txn in response:
                if txn['TransactionType'] == 'OfferCreate':
                    row_entry = self._parse_txn(txn, row_base)
                    ledger_entries.append(row_entry)


        except Exception as e:
            print(e)
            row_base['Error'] = e
            ledger_entries.append(row_base)
        
        return ledger_entries
    


    def _parse_txn(self, txn, row_base):
        row_entry = row_base.copy()

        row_entry.update({
            'Ledger_Date': txn['date'],
            'Transaction_Hash': txn['hash'],
            'Transaction_Fee': txn['Fee'],
            'Result': txn['meta']['TransactionResult'],
            'Account_ID': txn['Account'],

            'Gets_Currency_Code': txn['TakerGets']['currency'],
            'Gets_Count': txn['TakerGets']['value'],
            'Gets_Currency_Issuer': txn['TakerGets'].get('issuer', None),

            'Pays_Currency_Code': txn['TakerPays']['currency'],
            'Pays_Count': txn['TakerPays']['value'],
            'Pays_Currency_Issuer': txn['TakerPays'].get('issuer', None)
        })

        return row_entry
    
    def csv_filename(self, addition):
        today = dt.datetime.now().strftime('%Y-%m-%d')
        return f'{addition}_{today}.csv'

    def save_data(self, data, filename):
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False)



if __name__ == '__main__':
    collector = OfferCreateDataCollector()
    start_time = dt.datetime.now()
    

    newest_ledger = 92399495
    data_points = 100000

    print('Begin Data Collection')
    final_data = []
    j = 0
    for i in range(data_points):
        delta = i*500
        ledger_data = collector.get_data(newest_ledger - delta)
        final_data.extend(ledger_data)

        j+=1
        print(f'Completed {j}/{data_points}')

    print('Save to CSV')
    filename = collector.csv_filename('Offer_Create')
    collector.save_data(final_data, filename)

    end_time = dt.datetime.now()
    print(f'Runtime: {end_time - start_time}')
    print('Data Collection Complete')
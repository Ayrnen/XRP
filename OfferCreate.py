import requests
import pandas as pd


class OfferCreateDataCollector():
    def __init__(self):
        self.base_url = 'https://api.xrpscan.com/api/v1'
        self.headers = {
            'Authorization': '',
            'User-Agent': 'OfferCreateCollection/0.0.1',
        }


    def get_data(self, ledger_number):

        endpoint = f'/ledgers/{ledger_number}/transactions'
        url = self.base_url + endpoint

        
        ledger_entries = []
        row_base = {'Ledger_Number': ledger_number}

        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()

            for txn in response.json():
                if txn['TransactionType'] == 'OfferCreate':
                    row_entry = self._parse_transaction(txn, row_base)
                    ledger_entries.append(row_entry)


        except:
            row_base['Error'] = Exception
            ledger_entries.append(row_base)
        
        return ledger_entries
    


    def _parse_txn(self, txn, row_base):
        row_entry = row_base.copy()

        row_entry.update({
            'Transaction_Hash': txn['hash'],
            'Transaction_Fee': txn['Fee'],
            'Account_ID': txn['Account'],

            'Gets_Currency_Code': txn['TakerGets']['currency'],
            'Gets_Count': txn['TakerGets']['value'],
            'Gets_Currency_Issuer':txn['TakerGets'].get('issuer', None),

            'Pays_Currency_Code': txn['TakerPays']['currency'],
            'Pays_Count': txn['TakerPays']['value'],
            'Pays_Currency_Issuer':txn['TakerPayers'].get('issuer', None)
        })

        return row_entry
    
    def save_data(self, data):
        df = pd.DataFrame(data)
        df.to_csv('OfferCreateData.csv', index=False)



if __name__ == '__main__':
    collector = OfferCreateDataCollector()

    newest_ledger = 92399495
    desired_data_points = 5

    final_data = []
    for i in range(desired_data_points):
        ledger_data = collector.get_data(newest_ledger - i)
        final_data.extend(ledger_data)

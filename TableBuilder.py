import pandas as pd
import datetime as dt

class TableTransformer():
    def __init__(self):
        today = dt.datetime.now().strftime('%Y-%m-%d')
        self.df = pd.read_csv(f'Offer_Create_{today}.csv')
        self.df = self.df[self.df['Result'] == 'tesSUCCESS']

    def currency_mapper(self):
        gets_mapped = self.df[['Gets_Currency_Code', 'Gets_Currency_Issuer']].drop_duplicates()
        gets_mapped.columns = ['Code', 'Issuer']
        pays_mapped = self.df[['Pays_Currency_Code', 'Pays_Currency_Issuer']].drop_duplicates()
        pays_mapped.columns = ['Code', 'Issuer']

        result = pd.concat([gets_mapped, pays_mapped]).drop_duplicates().reset_index(drop=True)
        return result

    def ledger_metadata(self):
        result = self.df.groupby(['Ledger_Number', 'Ledger_Date']).agg(
            Row_Count=('Transaction_Hash', 'count'),
            Transaction_Fee_med=('Transaction_Fee', 'median'),
            Transaction_Fee_avg=('Transaction_Fee', 'mean'),
            Transaction_Fee_max=('Transaction_Fee', 'max'),
            Transaction_Fee_min=('Transaction_Fee', 'min'),
            Transaction_Fee_sum=('Transaction_Fee', 'sum'),
        )
        return result.reset_index()
    
    def ledger_account_metadata(self):
        result = self.df.groupby(['Ledger_Number', 'Ledger_Date', 'Account_ID']).agg(
            Row_Count=('Transaction_Hash', 'count'),
            Transaction_Fee_med=('Transaction_Fee', 'median'),
            Transaction_Fee_avg=('Transaction_Fee', 'mean'),
            Transaction_Fee_max=('Transaction_Fee', 'max'),
            Transaction_Fee_min=('Transaction_Fee', 'min'),
            Transaction_Fee_sum=('Transaction_Fee', 'sum'),
        )
        return result.reset_index()

    def ledger_currency_pairs(self):
        result = self.df.groupby(['Ledger_Number', 'Ledger_Date', 'Gets_Currency_Code', 'Pays_Currency_Code']).agg(

            Row_Count=('Transaction_Hash', 'count'),

            Transaction_Fee_med=('Transaction_Fee', 'median'),
            Transaction_Fee_avg=('Transaction_Fee', 'mean'),
            Transaction_Fee_max=('Transaction_Fee', 'max'),
            Transaction_Fee_min=('Transaction_Fee', 'min'),
            Transaction_Fee_sum=('Transaction_Fee', 'sum'),

            Gets_Count_med=('Gets_Count', 'median'),
            Gets_Count_avg=('Gets_Count', 'mean'),
            Gets_Count_max=('Gets_Count', 'max'),
            Gets_Count_min=('Gets_Count', 'min'),
            Gets_Count_sum=('Gets_Count', 'sum'),

            Pays_Count_med=('Pays_Count', 'median'),
            Pays_Count_avg=('Pays_Count', 'mean'),
            Pays_Count_max=('Pays_Count', 'max'),
            Pays_Count_min=('Pays_Count', 'min'),
            Pays_Count_sum=('Pays_Count', 'sum'),
        )
        return result.reset_index()

    def account_currency_pairs(self):
        result = self.df.groupby(['Account_ID', 'Gets_Currency_Code', 'Pays_Currency_Code']).agg(

            Row_Count=('Transaction_Hash', 'count'),

            Transaction_Fee_med=('Transaction_Fee', 'median'),
            Transaction_Fee_avg=('Transaction_Fee', 'mean'),
            Transaction_Fee_max=('Transaction_Fee', 'max'),
            Transaction_Fee_min=('Transaction_Fee', 'min'),
            Transaction_Fee_sum=('Transaction_Fee', 'sum'),

            Gets_Count_med=('Gets_Count', 'median'),
            Gets_Count_avg=('Gets_Count', 'mean'),
            Gets_Count_max=('Gets_Count', 'max'),
            Gets_Count_min=('Gets_Count', 'min'),
            Gets_Count_sum=('Gets_Count', 'sum'),

            Pays_Count_med=('Pays_Count', 'median'),
            Pays_Count_avg=('Pays_Count', 'mean'),
            Pays_Count_max=('Pays_Count', 'max'),
            Pays_Count_min=('Pays_Count', 'min'),
            Pays_Count_sum=('Pays_Count', 'sum')
        )
        return result.reset_index()
    
    def ledger_account_currency_pairs(self):
        result = self.df.groupby(['Ledger_Number', 'Ledger_Date', 'Account_ID', 'Gets_Currency_Code', 'Pays_Currency_Code']).agg(

            Row_Count=('Transaction_Hash', 'count'),

            Transaction_Fee_med=('Transaction_Fee', 'median'),
            Transaction_Fee_avg=('Transaction_Fee', 'mean'),
            Transaction_Fee_max=('Transaction_Fee', 'max'),
            Transaction_Fee_min=('Transaction_Fee', 'min'),
            Transaction_Fee_sum=('Transaction_Fee', 'sum'),

            Gets_Count_med=('Gets_Count', 'median'),
            Gets_Count_avg=('Gets_Count', 'mean'),
            Gets_Count_max=('Gets_Count', 'max'),
            Gets_Count_min=('Gets_Count', 'min'),
            Gets_Count_sum=('Gets_Count', 'sum'),

            Pays_Count_med=('Pays_Count', 'median'),
            Pays_Count_avg=('Pays_Count', 'mean'),
            Pays_Count_max=('Pays_Count', 'max'),
            Pays_Count_min=('Pays_Count', 'min'),
            Pays_Count_sum=('Pays_Count', 'sum')
        )
        return result.reset_index()
    
    def csv_filename(self, addition):
        today = dt.datetime.now().strftime('%Y-%m-%d')
        return f'{addition}_{today}.csv'

if __name__ == '__main__':
    print('Begin Data Transformation')
    transformer = TableTransformer()
    start_time = dt.datetime.now()
    
    print('Currency Mapper')
    df = transformer.currency_mapper()
    filename = transformer.csv_filename('Currency_Mapping')
    df.to_csv(filename, index=False)

    print('Ledger Metadata')
    df = transformer.ledger_metadata()
    filename = transformer.csv_filename('Ledger_Metadata')
    df.to_csv(filename, index=False)

    print('Ledger/Account Metadata')
    df = transformer.ledger_account_metadata()
    filename = transformer.csv_filename('Ledger_Account_Metadata')
    df.to_csv(filename, index=False)

    print('Ledger Currency Pairs')
    df = transformer.ledger_currency_pairs()
    filename = transformer.csv_filename('Ledger_Currency_Pairs')
    df.to_csv(filename, index=False)

    print('Account Currency Pairs')
    df = transformer.account_currency_pairs()
    filename = transformer.csv_filename('Account_Currency_Pairs')
    df.to_csv(filename, index=False)

    print('Ledger/Account Currency Pairs')
    df = transformer.ledger_currency_pairs()
    filename = transformer.csv_filename('Ledger_Account_Currency_Pairs')
    df.to_csv(filename, index=False)


    end_time = dt.datetime.now()
    print(f'Runtime: {end_time - start_time}')
    print('Data Transformation Complete')
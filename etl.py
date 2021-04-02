"""
Author: Kamrul Hasan
Email: hasana.alive@gmail.com
Date: 11.02.2021
"""

"""
bbg-etl.py
~~~~~~~~~~
This Python module contains an  ETL job definition
that implements reading data and transform using pandas and write to google sheet using google sheet API v4. It can be run as a simple python file. 

"""

# import the necessary dependencies for reading data and transformation
import pandas as pd
import config as cfg
import numpy as np
import gspread
from typing import Tuple
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient import discovery
from typing import Any, Callable, TypeVar, Union, List

T = TypeVar('T')


class DecoratorFactory():
    """
    This is a Decorator class for adding extra behaviour to the methods. Several decorator can be defined here and used later depends on the purpose of the methods.
    """
    F = TypeVar('F', bound=Callable[..., Any])

    @classmethod
    def calculate_printer(self, func: F) -> F:
        """
        Generic decorator fucntion to show when the function will be running
        """

        def new_function(*args):
            print(f"Calcualting: {func.__name__}")
            output = func(*args)
            print("Finished")
            return output

        return new_function

    @classmethod
    def gsapi_connection(self, func: F) -> F:
        """
        Generic decorator fucntion to show when the function will be running
        """

        def new_function(*args):
            print(f"GoogleSheet API: {func.__name__}")
            output = func(*args)
            print("Finished")
            return output

        return new_function

    @classmethod
    def gsapi_printer(self, func: F) -> F:
        """
        Generic decorator fucntion to show when the function will be running
        """

        def new_function(*args):
            print(f"GoogleSheet API: {func.__name__}")
            output = func(*args)
            print("Finished")
            return output

        return new_function


class DataFactory():
    """
    This class is specific for reading and checking the data.
    """

    def order_df(self) -> pd.DataFrame:
        """
        Reading the .xlxs data with openpyxl engine and pandas
        """
        path = cfg.order_data_path['order_data_path']
        try:
            df = pd.read_excel(path, engine='openpyxl')
        except IOError as e:
            print("invalid path")

        df.order_date = pd.to_datetime(df['order_date'])
        return df

    def cost_df(self) -> pd.DataFrame:
        """
        Reading the .xlxs data with openpyxl engine and pandas
        """
        path = cfg.cost_data_path['cost_data_path']
        try:
            df = pd.read_excel(path, engine='openpyxl')
        except IOError as e:
            print("invalid path")
        return df

    def id_df(self) -> pd.DataFrame:
        """
        Reading the .csv data with pandas
        """
        path = cfg.id_data_path['id_data_path']
        try:
            df = pd.read_csv(path, delimiter=";")
        except IOError as e:
            print("invalid path")

        return df


class OrderCost():
    """
        This class is specific for transforming data like joining or merging for get the expected outcome according to the requirements. This is kind of initial transformation/cleaning layer of the data.
    """

    def __init__(self) -> None:
        self._data = DataFactory()

    @property
    def order_id_df(self) -> pd.DataFrame:
        """
        Merging order dataframe and id dataframe to get the mapping of shop id to actual shop name.
        """
        # left join with order data table with shop data table
        df = self._data.order_df().merge(self._data.id_df(), left_on='shop_id', right_on='ID') \
            .drop(columns="ID").rename({'Shop   Name  ': 'shop_name'}, axis=1)
        # get all data consists of shop name Auna and Numan, excluding the country code
        df['shop_only_name'] = df['shop_name'].str.extract('(Auna|AUNA|Numan|NUMAN)')
        # making uppercase to all the shope name with lambda expression
        df.shop_only_name = df['shop_only_name'].apply(lambda x: x.upper())
        df['revenue_after_discount'] = df['revenue_before_discount'] - df['discount']
        self._order_id_df = df
        return self._order_id_df

    @property
    def cost_id_df(self) -> pd.DataFrame:
        """
        Merging cost dataframe and id dataframe to get the mapping of shop id to actual shop name.
        """
        df = self._data.cost_df()
        df.advertising_costs = df['advertising_costs'].replace(np.nan, 0)
        df.advertising_costs = df['advertising_costs'].apply(pd.to_numeric, errors='coerce')
        # left join with cost data table with shop data table
        df = (df.merge(self._data.id_df(), left_on='shop_id', right_on='ID')).drop(columns="ID").rename(
            {'Shop   Name  ': 'shop_name'}, axis=1)
        # get all data consists of shop name Auna and Numan, excluding the country code
        df['shop_only_name'] = df['shop_name'].str.extract('(Auna|AUNA|Numan|NUMAN)')
        # making uppercase to all the shope name with lambda expression
        df.shop_only_name = df['shop_only_name'].apply(lambda x: x.upper())
        self._cost_id_df = df
        return self._cost_id_df

    @property
    def revenues(self) -> T:
        return RevenueFactory(self)

    @property
    def cost_revenue_ratio(self) -> T:
        return CostRevenueFactory(self)

    @property
    def gs_api(self) -> T:
        return GsApiFactory(self)


class RevenueFactory():
    """
    This class is specific for extract information like revenue, share or unique customer from the dataframe according to the requirements.
    """

    def __init__(self, order_cost: T) -> None:
        self._order_cost = order_cost

    @DecoratorFactory.calculate_printer
    def total_revenue(self) -> pd.DataFrame:
        """
        Get the total revenue of all shops after discount
        """
        df = self._order_cost.order_id_df
        df = round(sum(df["revenue_after_discount"]), 2)
        df = pd.DataFrame({"Total Revenue": df}, index=[0])
        self._total_revenue_df = df
        return self._total_revenue_df

    @DecoratorFactory.calculate_printer
    def number_unique_customers(self) -> pd.DataFrame:
        """
        Get the Total Number of Unique Customers
        """
        df = self._order_cost.order_id_df
        df = len(df.customer_id.unique())
        df = pd.DataFrame({"Total Unique Customers": df}, index=[0])
        return df

    @DecoratorFactory.calculate_printer
    def aun_numan_revenue(self) -> pd.DataFrame:
        """
        Get the Total Revenue (after discount), broken down by brand (Auna & Numan)
        """
        df = self._order_cost.order_id_df
        df = df.groupby('shop_only_name')[["revenue_after_discount"]].sum().round(2)
        df = df.reset_index()
        return df

    @DecoratorFactory.calculate_printer
    def webshop_revenue(self) -> pd.DataFrame:
        """
        Get the Total Revenue (after discount), broken down by shop (web shop)
        """
        df = self._order_cost.order_id_df
        df = df.groupby('shop_id')[["revenue_after_discount"]].sum().round(2)
        df = df.reset_index()
        return df

    @DecoratorFactory.calculate_printer
    def share_revenue(self) -> pd.DataFrame:
        """
        Get the Revenue Share (after discount) of each category (as a percentage of total
        revenue)
        """
        df = self._order_cost.order_id_df
        df = df.groupby('product_category')[["revenue_after_discount"]].sum()
        df = df['revenue_after_discount'].transform(lambda x: ((x / x.sum()) * 100).round(2))
        df = df.reset_index()
        df = df.rename({'revenue_after_discount': 'revenue_share(%)'}, axis=1)
        return df

    @DecoratorFactory.calculate_printer
    def top_5_customers(self) -> None:
        """
        Get the Top 5 Customers, in terms of repeated purchases
        """
        df = self._order_cost.order_id_df
        df = df.groupby(['customer_id'])['repeated_purchases'].sum()
        df = df.sort_values(ascending=False).head(5)
        df = df.reset_index()
        df = df.rename({'customer_id': 'Top 5 Customers'}, axis=1)
        return df


class CostRevenueFactory():
    T = TypeVar('T')

    def __init__(self, order_cost: T) -> None:
        self._order_cost = order_cost

    @DecoratorFactory.calculate_printer
    def total_crr(self) -> pd.DataFrame:
        df = self._order_cost.cost_id_df
        df = ((df['advertising_costs'].sum() / self._order_cost.revenues.total_revenue()) * 100).round(2)
        df = df.rename({'Total Revenue': 'Total CRR(%)'}, axis=1)
        return df

    @DecoratorFactory.calculate_printer
    def aun_numan_crr(self) -> pd.DataFrame:
        df_cost = self._order_cost.cost_id_df
        df_order = self._order_cost.order_id_df
        df_cost = df_cost.groupby(['date', 'shop_only_name'])['advertising_costs'].sum()
        df_order = df_order.groupby(['order_date', 'shop_only_name'])['revenue_after_discount'].sum()
        df = pd.concat([df_cost, df_order], axis=1)
        df['advertising_costs'] = df['advertising_costs'].fillna(0)
        df['revenue_after_discount'] = df['revenue_after_discount'].fillna(0)
        df['CRR(%)'] = (df['advertising_costs'] / df['revenue_after_discount']).round(3)
        df = df.reset_index()
        df = df.rename({'level_0': 'date', "level_1": "shop_name"}, axis=1)
        df = df.drop(columns=['advertising_costs', 'revenue_after_discount'], axis=1)
        df['date'] = df['date'].dt.strftime('%d-%m-%Y')
        df.replace([np.inf, np.nan], 0, inplace=True)

        return df


class GsApiFactory():
    """
    This class is specific for accesing all the endpoints of Google sheet api v4.
    """

    def __init__(self, order_cost: T) -> None:
        self._order_cost = order_cost

    @DecoratorFactory.gsapi_connection
    def connection(self) -> Tuple[T, T]:
        """
        Establish the Google Sheet v4 API connection with the credentials from google cloud
        """
        SCOPES = ['https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name('./mycredentials.json', SCOPES)
        service = discovery.build('sheets', 'v4', credentials=creds)
        return service, creds

    @DecoratorFactory.gsapi_printer
    def create_spread_sheet(self, service: T, creds: T, title: str) -> str:
        """
        Create a new spread sheet if needed with any name
        """
        spreadsheet = {
            'properties': {
                'title': title
            }
        }
        spreadsheet = service.spreadsheets().create(body=spreadsheet, fields='spreadsheetId').execute()
        # print('Spreadsheet ID: {0}'.format(spreadsheet.get('spreadsheetId')))
        return spreadsheet.get('spreadsheetId')

    @DecoratorFactory.gsapi_printer
    def permission(self, email: str, creds: T, spreadsheetId: str) -> None:
        """
        Giving a permission to read, write to any new user with their email
        """

        user_permission = ({'type': 'user',
                            'role': 'writer',
                            'emailAddress': email
                            })
        drive_service = discovery.build('drive', 'v3', credentials=creds)
        drive_service.permissions().create(fileId=spreadsheetId,
                                           body=user_permission,
                                           fields='id').execute()

    @DecoratorFactory.gsapi_printer
    def create_work_sheet(self, service: T, title: str, spreadsheetId: str) -> None:
        """
        Create a new work sheet if needed with any name
        """

        # call the Sheets API
        body = {'requests': [
            {
                'addSheet': {
                    'properties': {
                        'title': title
                    }
                }
            }]}
        request = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheetId,
            body=body)
        response = request.execute()

    @DecoratorFactory.gsapi_printer
    def write_data(self, service, data: List[List[Union[str, int]]], range_: str, spreadsheetId: str) -> None:
        '''
        Writing the data first time in a work sheet with api update endpoint
        '''
        values = data
        body = {
            'range': range_,
            'values': values,
            'majorDimension': 'ROWS'
        }
        request = service.spreadsheets().values().update(
            spreadsheetId=spreadsheetId,
            range=range_,
            valueInputOption='USER_ENTERED',
            body=body)
        response = request.execute()

        # append

    @DecoratorFactory.gsapi_printer
    def update_data(self, service, data: List[List[Union[str, int]]], range_: str, spreadsheetId) -> None:
        '''
       Updating the data first time in a existing work sheet with api append endpoint
        '''
        # range_ = 'MySheet!A:C'
        values = data
        body = {
            'range': range_,
            'values': values,
            'majorDimension': 'ROWS'
        }
        request = service.spreadsheets().values().append(
            spreadsheetId=spreadsheetId,
            range=range_,
            valueInputOption='USER_ENTERED',
            insertDataOption='INSERT_ROWS',
            body=body)
        response = request.execute()

    @DecoratorFactory.gsapi_printer
    def clear_data(self, service: T, sheetName: str, range_: str, spreadsheet_id: str) -> None:
        '''
        Clear the entire work sheet with the name as a parameter
        '''
        body = {}
        service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=range_,
                                              body=body).execute()


def main() -> None:
    '''
        This is a main function to run the whole pipeline 
    '''
    oc = OrderCost()
    # Getting all the metrics and KPI as dataframe and keeping in a list 
    df = []
    df.append(oc.revenues.total_revenue())
    df.append(oc.revenues.number_unique_customers())
    df.append(oc.revenues.aun_numan_revenue())
    df.append(oc.revenues.webshop_revenue())
    df.append(oc.revenues.share_revenue())
    df.append(oc.revenues.top_5_customers())
    df.append(oc.cost_revenue_ratio.total_crr())
    df.append(oc.cost_revenue_ratio.aun_numan_crr())

    # Establishing the API connection 
    service, creds = oc.gs_api.connection()

    # for already exisitng spread sheet we can use the spread sheed uniquie identifier or we can laso create a new
    # spreadsheet with the api and create_spread_sheet function by giving any name titel = "BBG_Report" spreadSheetId
    # = oc.gs_api.create_spread_sheet(service,creds,titel)
    spreadSheetId = "1j0zhJTMSgTpDsXdVqfb2DH0pYc9HZ7cM6DQwwDOJs24"

    # giving the permission to new user with the email.
    # oc.gs_api.permission("hasan.alive@gmail.com",creds,spreadSheetId)

    # rnage to select rows to write the data
    range_ = 'Sheet1!A:C'

    # for already exisitng work sheet we can use the spread sheed name or we can laso create a new spreadsheet with
    # the api and create_spread_sheet function
    work_sheet_title = "Sheet1"

    # clear any existing data before writing
    oc.gs_api.clear_data(service, work_sheet_title, range_, spreadSheetId)

    # iterate through the metric list and write each metric into the google sheet though the api functions
    for index, item in enumerate(df):
        if index == 0:
            data = ([item.columns.values.tolist()] + item.values.tolist())
            # write function to write the data
            oc.gs_api.write_data(service, data, range_, spreadSheetId)
        else:
            range_ = 'Sheet1!A:C'
            empty_cell = ["", ""]
            data = ([item.columns.values.tolist()] + item.values.tolist())
            data.insert(0, empty_cell)
            data.insert(0, empty_cell)

            # append function to append new data
            oc.gs_api.update_data(service, data, range_, spreadSheetId)


if __name__ == '__main__':
    # Trigger the pipeline through main function!

    main()

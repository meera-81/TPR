# Python Function for Consolidating and Classifying Multi-Year Pensions Data
# Function to import pensions data from excel, cleanse and transform the data, and export back to excel
import pandas as pd

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


def classify_membership(members):
    if members < 5000:
        return '<5k members'
    elif members < 50000:
        return 'Between 5k & 50k members'
    elif members < 500000:
        return 'Between 50k & 500k members'
    else:
        return '>500k members'


def classify_assets(assets):
    if assets < 10000000:
        return '< £10M'
    elif assets < 100000000:
        return '£10M – £100M'
    elif assets < 1000000000:
        return '£100M – £1B'
    else:
        return '>1B'


def import_pensions_data(header_row=0):
    """
        Reads all sheets from the Pensions file except the first,
        appends them, and adds a 'year' column based on sheet names.

        Parameters:
        - header_row: int, row index for column headers

        Returns:
        - df_all: Combined DataFrame
        """

    input_file_path = 'C:/Users/Meera/DA/TPR/Senior Analyst Interview Task.xlsx'
    output_file_path = 'C:/Users/Meera/DA/TPR/All_pensions_data.xlsx'
    excel_file = pd.ExcelFile(
        input_file_path)  # Reads the file into memory, used for inspecting available sheet names and selective loading.
    all_sheets = excel_file.sheet_names[
                 1:]  # Gets a list of all sheet names, starting from index 1 to exclude the first sheet

    df_list = []
    for sheet_name in all_sheets:
        df = pd.read_excel(input_file_path, sheet_name=sheet_name, header=header_row)
        df['year'] = sheet_name

        df['Assets'] = (
            df['Assets'].astype(str).str.replace('£', '')  # Remove pound sign
            .astype(float)  # Convert back to numeric
        )
        df['Memberships'] = pd.to_numeric(df['Memberships'], errors='coerce').round().astype(
            'Int64')  # Convert string to int, replace blanks with Na
        df_list.append(df)

    df_all = pd.concat(df_list, ignore_index=True)  # Combines all yearly sheets into one unified DataFrame.
    df_filtered = df_all.drop_duplicates().copy()  # Remove duplicates (26)

    # Calculate AssetPerMember ratio
    # Initialize column with NaNs
    df_filtered['Asset_per_Member'] = pd.NA

    # Compute ratio only where both values are valid and positive
    valid_mask = (df_filtered['Memberships'] > 0) & (df_filtered['Assets'] > 0)
    df_filtered.loc[valid_mask, 'Asset_per_Member'] = (
            df_filtered.loc[valid_mask, 'Assets'] / df_filtered.loc[valid_mask, 'Memberships']
    ).round(2)

    # Scheme classification based on membership size (calculate average membership size per scheme across all years)
    scheme_membership = (
        df_filtered.groupby('PSR')['Memberships']
        .mean()
        .reset_index()
        .rename(columns={'Memberships': 'AvgMemberships'})
        .round({'AvgMemberships': 0})
    )
    scheme_membership['MembershipSizeCategory'] = scheme_membership['AvgMemberships'].apply(classify_membership)

    # Merge scheme classification back to filtered dataframe
    df_scheme_size = df_filtered.merge(scheme_membership[['PSR', 'AvgMemberships', 'MembershipSizeCategory']], on='PSR',
                                       how='left')

    # Scheme classification based on Asset value (calculate average asset value per scheme across all years)
    scheme_assets = (
        df_filtered.groupby('PSR')['Assets']
        .mean()
        .reset_index()
        .rename(columns={'Assets': 'AvgAssets'})
        .round({'AvgAssets': 0})
    )
    scheme_assets['AssetValueCategory'] = scheme_assets['AvgAssets'].apply(classify_assets)

    # Merge scheme classification back to filtered dataframe
    df_pension_scheme = df_scheme_size.merge(scheme_assets[['PSR', 'AvgAssets', 'AssetValueCategory']], on='PSR',
                                             how='left')

    # Identify Discontinued pension schemes
    # Get the latest active year and merge back to original dataset
    last_active_years = df_pension_scheme.groupby('PSR')['year'].max().reset_index()
    last_active_years.columns = ['PSR', 'last_active_year']

    last_active_years['last_active_year'] = last_active_years['last_active_year'].astype(int)
    final_year = 2025
    last_active_years['discontinued_year'] = last_active_years['last_active_year'].apply(
        lambda year: year + 1 if year < final_year else None
    )
    df_final = df_pension_scheme.merge(last_active_years, on='PSR', how='left')

    # Export data
    df_final.to_excel(output_file_path, index=False)


import_pensions_data()

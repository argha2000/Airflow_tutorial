
def remove_csv_files():
    import os
    import glob
    
    path = "/store_files_mysql/transfer_to_business_files/*.csv"
    files = glob.glob(path)
    if files:
        for file in files:
            try:
                os.remove(file)
                print(f"Successfully removed {file}")
            except Exception as e:
                print(f"Error removing {file}: {str(e)}")
    else:
        print("No CSV files found to remove")
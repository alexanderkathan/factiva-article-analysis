import os
import pandas as pd

seasons = ['2017-18']

for season in seasons:
    print(season)
    folder_path = f"/mnt/nas/data_work/AK/Leader-Humor/articles/output_{season}"

    files = os.listdir(folder_path)

    file_list = []

    for f in files:
        df = pd.read_csv(os.path.join(folder_path,f),index_col=False)
        file_list.append(df)

    output = pd.concat(file_list,axis=0)
    output.to_csv(os.path.join(f"/mnt/nas/data_work/AK/Leader-Humor/articles/output_{season}",f"articles_{season}.csv"),index=False)
    output.to_excel(os.path.join(f"/mnt/nas/data_work/AK/Leader-Humor/articles/output_{season}",f"articles_{season}.xlsx"),index=False)

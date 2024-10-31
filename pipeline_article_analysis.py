import os

seasons = ['2017-18']

for season in seasons:
	print("="*200)
	print(f"Season: {season}")
	print('-'*200)
	input_dir = f"/mnt/nas/data_work/AK/Leader-Humor/articles/{season}"
	output_dir = f"/mnt/nas/data_work/AK/Leader-Humor/articles/output_{season}" # output_{season}_additional

	if not os.path.exists(output_dir):
		os.makedirs(output_dir)
	all_input_files = os.listdir(input_dir)
	already_processed_files = os.listdir(output_dir)
	already_processed_files = [i.replace(".csv",".html") for i in already_processed_files]

	for ind,input_article_file in enumerate(all_input_files):
		if '.html' in input_article_file:
			if input_article_file not in already_processed_files:
				try:
					print(f"     - Processing file {ind+1}/{len(all_input_files)}: {input_article_file}")
					os.system(fr"python /home/kathanal/projects/Humor/Articles/check_articles_final.py --input={input_dir} --output={output_dir} --file={input_article_file}")
					os.system("rm -r /home/kathanal/projects/Humor/Articles/pd_index")
				except Exception as e:
					print(f"     - Error with file {input_article_file}: {e}")
				#break


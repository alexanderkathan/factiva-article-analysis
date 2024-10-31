import pandas as pd
import os
from striprtf.striprtf import rtf_to_text
from germansentiment import SentimentModel
import pyterrier as pt
from bs4 import BeautifulSoup
import pysbd
import argparse
from datetime import datetime


def get_data_df(file_name):
    with open(file_name) as fp:
        results = BeautifulSoup(fp, 'html.parser')
    doc_no_list = []
    title_list = []
    newspaper_list = []
    date_list = []
    text_list = []
    articles = results.find_all('div', class_='article deArticle')
    num = 0
    for ind,entry in enumerate(articles):
        seg = pysbd.Segmenter(language="de", clean=False)
        title = entry.find_all('span', class_='deHeadline')[0]
        paragraphs = entry.find_all('p', class_='articleParagraph dearticleParagraph')
        body = ""
        for paragraph in paragraphs:
            text = paragraph.text
            text = text.replace('Frankfurter Allgemeine Zeitung','FAZ')
            text = text.replace('ä','ae')
            text = text.replace('ü','ue')
            text = text.replace('ö','oe')
            text = text.replace('ß','ss')
            text = text.lower()
            text = text.replace('1. fc','fc')
            if body=="":
                body = text
            else:
                body += " " + text
        if 'frauen' not in body and 'damen' not in body and 'spielerin' not in body:
            text_array = seg.segment(body)
            # text_array = re.split('[.;!] |\n', body)
            for body in text_array:
                if len(body.split(" "))<4:
                    continue
                if "\n" in title.text[:1]:
                    #print(title.text)
                    title_text = title.text[1:]
                    #print(title_text)
                    #print('---------------')
                else:
                    title_text = title.text
                title_list.append(title_text)
                text_list.append(body)
                newspaper = entry.find_all('p')[-2]
                newspaper_list.append(newspaper.text)
                date = entry.find_all('div')[:7]
                for div in date:
                    if ('2017' in div.text or '2018' in div.text or '2019' in div.text or '2020' in div.text or '2021' in div.text or '2022' in div.text) and 'words' not in div.text and 'Copyright' not in div.text:
                        date = div.text
                date_list.append(date)
                num += 1
                doc_no_list.append(str(num))
    df = pd.DataFrame({'docno': doc_no_list, 'title': title_list, 'newspaper': newspaper_list, 'date': date_list, 'text': text_list})
    #print(df)
    return df


def check_relevance(row, columns):
    for column in columns:
        if 'score' in column:
            if row[column]>2:
                return 'yes'
    return 'no'


def calculate_text_relevance(df):
    if not os.path.exists("/home/kathanal/projects/Humor/Articles/pd_index"):
        pd_indexer = pt.DFIndexer("/home/kathanal/projects/Humor/Articles/pd_index")
        indexref = pd_indexer.index(df["text"], df["docno"])
    index = pt.IndexFactory.of("/home/kathanal/projects/Humor/Articles/pd_index/data.properties")
    bm25 = pt.BatchRetrieve(index, wmodel="BM25")
    pl2 = pt.BatchRetrieve(index, wmodel="PL2")
    pipeline = (bm25 % len(df)) >> pl2

    df_teams = pd.read_csv('/home/kathanal/projects/Humor/Articles/teams.csv')
    df_list = []
    df = df.set_index('docno')
    df_list.append(df)
    for team in df_teams['team'].unique():
        all_synonyms = ' '.join(df_teams[df_teams['synonym']==team]['synonym'].values)
        query = f"+{team}^2 + {all_synonyms} -frauen^10 -damen^10 -spielerin^10 -Frauen -Damen -Spielerin"
        relevance_df = pipeline(pd.DataFrame({'qid': [1], 'query': [query]}))
        relevance_df = relevance_df.set_index('docno')
        relevance_df = relevance_df[['score']]
        relevance_df = relevance_df.rename(columns={'score': f'score_{team}'})
        df_list.append(relevance_df)
        #print(relevance_df)
    final_df = pd.concat(df_list, axis=1)
    final_df['relevant'] = final_df.apply(lambda x: check_relevance(x, final_df.columns), axis=1)
    return final_df


def get_text_sentiment(model, text, nlp):
    df = pd.read_csv('/home/kathanal/projects/Humor/Articles/players.csv')
    all_teams = df['team'].unique()
    result_array = []
    text = text.lower()
    #print(text)
    text_array = text #re.split('[.;!] |\n', text)
    #print(text_array)
    text_list = []
    sentiment_list = []
    for t in text:
        result = model.predict_sentiment([t])
        result_array.append(result[0])
        text_list.append(text)
        sentiment_list.append(result[0])
    text_snipped_df = pd.DataFrame({'text': text_list, 'sentiment': sentiment_list})
    return text_snipped_df


def count_sentiment(entry, sentiment):
    num = entry.count(sentiment)
    return num



if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=str)
    parser.add_argument('--output', type=str)
    parser.add_argument('--file', type=str)
    args = parser.parse_args()

    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
    pt.init()

    input_path = args.input
    output_path = args.output
    file_name = args.file

    t1 = datetime.now()
    df = get_data_df(os.path.join(input_path,file_name))

    model = SentimentModel()
    df = calculate_text_relevance(df)
    t2 = datetime.now()
    delta = t2-t1
    print(f"          Step 1: Load data and calculate Text relevance ({delta.total_seconds()} sec)")

    df_list = []
    txt_list = df['text'].values
    i = 0
    sentiment_list = []
    print("          Step 2: Predict sentiment")
    while i+100<len(txt_list):
        t1 = datetime.now()
        sentiment_list += model.predict_sentiment(txt_list[i:i+100])
        t2 = datetime.now()
        delta = t2-t1
        print(f"               {i}/{len(txt_list)} ({delta.total_seconds()} sec)")
        i += 100

    sentiment_list += model.predict_sentiment(txt_list[i:])
    df['sentiment'] = sentiment_list
    df.to_csv(os.path.join(output_path,file_name.replace(".html",".csv")),index=False)

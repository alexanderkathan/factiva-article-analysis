import pandas as pd
import pyterrier as pt
import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
pt.init()

# define an example dataframe of documents
import pandas as pd



df = pd.DataFrame({
    'docno':
    ['1', '2'],
    'text':
    ["Das erste Pokalfinale des Tages verlieren die Bayern	FAZ.net	19 May 2018 Copyright 2018 Frankfurter Allgemeine Zeitung GmbH. Provided by Frankfurter Allgemeine Zeitung Archiv  Caroline Hansen sank auf die Knie. Geschafft! Die 23-jährige Norwegerin hatte den entscheidenden Versuch im Elfmeterschießen verwandelt. Titelverteidiger VfL Wolfsburg hat dank dieses Treffers und der Paraden von Torhüterin Almuth Schult zum fünften Mal den DFB-Pokal der Frauen gewonnen. Die Mannschaft von Trainer Stephan Lerch setzte sich am Samstag im Finale in Köln gegen den FC BayernMünchen mit 3:2 im Elfmeterschießen durch und machte nach dem Gewinn des Meistertitels das Double perfekt. In den 120 Spielminuten waren keine Treffer gefallen. Interims-Frauen-Bundestrainer Horst Hrubesch ehrte die Siegerinnen. Spielplan der Frauenfußball-WM 2019 in Frankreich Wolfsburgs Sportdirektor Ralf Kellermann sagte: Wir waren über weite Strecken die bessere Mannschaft. Torhüterin Schult betonte, wie knapp der Erfolg war: Ich bin so froh, dass wir das Glück hatten. So ist es schön. Sie hatte zwei Elfmeter pariert, außerdem schoss die Münchnerin Kristin Demann an die Latte. So ekelhaft ist Fußball, stellte Bayern-Abwehrspielerin Simone Laudehr fest, die nach mehr als fünf Monaten Verletzungspause in der Verlängerung ihr Comeback gab. Für Wolfsburg trafen vor 17 692 Zuschauern im Rhein-Energie-Stadion neben Hansen auch Isabel Kerschowski und Pernille Harder vom Elfmeterpunkt, Mandy Islacker und Simone Laudehr erzielten die Münchner Treffer. Die nächste Chance auf einen Titel bietet sich Wolfsburg am kommenden Donnerstag im Champions-League-Finale in Kiew gegen den französischen Titelverteidiger Olympique Lyon. Zuletzt hatte es 2007 ein Elfmeterschießen im Pokalfinale gegeben. Damals gewann der FFC Frankfurt im Elfmeterschießen gegen den FCR Duisburg. Die beiden derzeit besten deutschen Vereinsteams schenkten sich in einer munteren Partie nichts. Nach dem Beginn mit vielen intensiven Zweikämpfen im Mittelfeld erspielten sich die Bayern die erste Torchance. Die Schwedin Fridolina Rolfö (14.) zwang Nationaltorhüterin Schult mit einem Distanzschuss zu einer Glanzparade. Fünf Minuten später war auch Münchens Keeperin Manuela Zinsberger aufmerksam und lenkte nach einem schnellen VfL-Konter über Claudia Neto einen Schuss von Hansen ins kurze Eck um den Pfosten. Auf der Gegenseite stand kurz darauf erneut Schult (21.) im Blickpunkt. Die DFB-Torfrau musste sich mächtig strecken, um einen 22-Meter-Heber von Nicole Rolser mit den Fingerspitzen noch an den Querbalken zu lenken. Nach und nach erarbeitete sich der Titelverteidiger leichte Feldvorteile und hatte durch Pajor kurz vor der Pause die größte Chance zur Führung. Doch die Polin konnte einen zu kurz geratenen Rückpass von Melanie Behringer nicht nutzen und scheiterte an der gut reagierenden Österreicherin Zinsberger. Auch nach dem Wechsel neutralisierten sich die Top-Teams weitgehend. Die Münchnerin Sara Däbritz (83.) prüfte Schult in der Endphase noch einmal. Auf der anderen Seite vergab Pernille Harder (88.) kurz vor Ende der regulären Spielzeit eine gute Möglichkeit. In der Verlängerung wurden flüssige Aktionen immer seltener.",
    "Nach dem Wirbel um die Erdogan-Fotos haben sich Mesut Özil und Ilkay Gündogan mit Bundespräsident Frank-Walter Steinmeier getroffen und auch ein klärendes Gespräch mit der DFB-Spitze geführt. Steinmeier teilte am Samstagabend mit, die beiden deutschen Fußball-Nationalspieler hätten den Wunsch geäußert, ihn zu besuchen. Es sei ihnen wichtig gewesen, entstandene Missverständnisse aus dem Weg zu räumen. Wir haben lange gesprochen, über Sport, aber auch über Politik, postete Steinmeier via Facebook nach dem Treffen im Garten von Schloss Bellevue. Özil und Gündogan hatten dem türkischen Staatspräsidenten Recep Tayyip Erdogan am vergangenen Sonntag in London Trikots ihrer Vereine FC Arsenal und Manchester City überreicht. Die von Erdogans Partei veröffentlichten Bilder hatten schnell ein harsches Echo ausgelöst. Auf dem Trikot, das Gündogan an Erdogan überreicht hatte, stand handschriftlich über der Signatur auf Türkisch: Für meinen verehrten Präsidenten - hochachtungsvoll"]
})


"""
df = pd.DataFrame({
    'docno':
    ['1', '2', '3', '4', '5'],
    'url':
    ['url1', 'url2', 'url3', 'url4', 'url5'],
    'text':
    ['He ran out of money, so he had to stop playing',
    'The waves were crashing on the shore; it was a',
    'The body may perhaps compensates for the loss',
    'Money is beautiful and has money loss',
    'Yes we love money indeed.']
})
"""

# index the text, record the docnos as metadata
#pd_indexer = pt.DFIndexer("./pd_index")
#indexref = pd_indexer.index(df["text"], df["docno"])


index = pt.IndexFactory.of("./pd_index/data.properties")
bm25 = pt.BatchRetrieve(index, wmodel="BM25")
pl2 = pt.BatchRetrieve(index, wmodel="PL2")
pipeline = (bm25 % 100) >> pl2
print(pipeline(pd.DataFrame({'qid': [1], 'query': ['+ball -frauen']})))


from language_identification import LanguageIdentification

response = [line[:-1] for line in open('response.txt', encoding='utf-8')]
LANGUAGE = LanguageIdentification()
dd = 0
for line in response:
    LANG = LANGUAGE.predict_lang(line)[0][0]
    if 'es' in LANG:
        dd += 1
print(dd, len(response))
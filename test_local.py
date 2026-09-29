from wikiindex import parse_article

valid_line = '{"id":10,"title":"MapReduce","text":"Maps data 1 ...","links":[20]}'
print("Valid JSON test:", parse_article(valid_line))

invalid_line = 'not-json'
print("Invalid JSON test:", parse_article(invalid_line))
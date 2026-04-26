import requests
import re
import json

html = requests.get('https://kep.nung.edu.ua/pages/education/schedule').text

match = re.search(r'"([А-ЯІЄЇA-Z0-9\-\(\)\| ]+)":\{', html)
if match:
    start_idx = match.start()
    with open('scratch/out2.txt', 'w', encoding='utf-8') as f:
        f.write(html[start_idx:start_idx+1500])

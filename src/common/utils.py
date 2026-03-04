import re

def clean_column_name(name):
    if not name:
        return "unnamed_column"

    name = name.lower().strip()

    name = name.replace('é', 'e').replace('è', 'e').replace('ê', 'e')\
               .replace('à', 'a').replace('â', 'a')\
               .replace('î', 'i').replace('ï', 'i')\
               .replace('ô', 'o').replace('û', 'u').replace('ù', 'u')\
               .replace('ç', 'c')

    name = re.sub(r'[^a-z0-9]', '_', name)

    name = re.sub(r'_+', '_', name)
    return name.strip('_')

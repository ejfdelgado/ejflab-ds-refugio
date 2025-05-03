import re
import datetime

month_map = {
    "enero": "January",
    "febrero": "February",
    "marzo": "March",
    "abril": "April",
    "mayo": "May",
    "junio": "June",
    "julio": "July",
    "agosto": "August",
    "septiembre": "September",
    "octubre": "October",
    "noviembre": "November",
    "diciembre": "December"
}

def parse_integer(number_str):
    if number_str is not None:
        match = re.search(r"^\s*([\d]+)\s*$", number_str)
        if match:
            return int(match.group(1))
    return None

def parse_spanish_date(date_str):
    if date_str is not None:
        # Formats:
        # martes, octubre 08, 2019 - 
        match1 = re.search(r"^([^,]+),\s*([^\s]+)\s*(\d+),\s*(\d*)$", date_str)
        if match1:
            month_day = match1.group(2)
            month = month_map.get(month_day.lower())
            day = match1.group(3)
            year = match1.group(4)
            formatted = f"{month} {day}, {year}"
            return datetime.datetime.strptime(formatted, "%B %d, %Y").date()
        else:
            # sábado, 28 de diciembre de 2019
            match2 = re.search(r"^([^,]+),\s*(\d+)\s+de\s+([^\s]+)\s+de\s+([\d]+)$", date_str)
            if match2:
                month_day = match2.group(3)
                month = month_map.get(month_day.lower())
                day = match2.group(2)
                year = match2.group(4)
                formatted = f"{month} {day}, {year}"
                return datetime.datetime.strptime(formatted, "%B %d, %Y").date()
    return None
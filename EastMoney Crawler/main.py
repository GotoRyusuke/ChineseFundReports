import requests
import json
import time
import xlrd
import os
Headers = {'Referer': 'http://fundf10.eastmoney.com/'}

s = requests.Session()
INTERVAL_INDEX = 0.7


def get_list(fund_code):
    try:
        resp = s.get(f'http://api.fund.eastmoney.com/f10/JJGG?fundcode={fund_code}&pageIndex=1&pageSize=99999&type=3'
                     ,headers=Headers, timeout=6)
        return resp.json()
    except Exception as e:
        print(str(e))
        return None


def get_pub_info(ID):
    try:
        resp = s.get(f'https://np-cnotice-fund.eastmoney.com/api/content/ann?client_source=web_fund'
                     f'&show_all=1&art_code={ID}', headers=Headers)
        return resp.json()
    except Exception as e:
        print(str(e))
        return None


def generate_pdf_url(ID: str):
    return f'https://pdf.dfcfw.com/pdf/H2_{ID}_1.pdf'


def generate_txt_url(ID: str):
    return f'https://pdf.dfcfw.com/pdf/H2_{ID}_1.txt'


def generate_doc_url(ID: str):
    return f'https://pdf.dfcfw.com/pdf/H2_{ID}_1.doc'


def get_pdf(ID: str):
    try:
        url = generate_pdf_url(ID)
        # print(url)
        resp = s.get(url, headers=Headers, timeout=6)
        return resp.content
    except Exception as e:
        print(str(e))
        return None


def get_doc(ID: str):
    try:
        url = generate_doc_url(ID)
        # print(url)
        resp = s.get(url, headers=Headers, timeout=6)
        return resp.content
    except Exception as e:
        print(str(e))
        return None


def get_txt(ID: str):
    try:
        url = generate_txt_url(ID)
        # print(url)
        resp = s.get(url, headers=Headers, timeout=6)
        return resp.content.decode('gbk', errors='ignore')
    except Exception as e:
        print(str(e))
        return None


def extract_info(data_json: dict) -> list:
    return [[i["FUNDCODE"], i["TITLE"], i["PUBLISHDATEDesc"], i["ID"], i["ATTACHTYPE"]] for i in data_json['Data']]


def load_fund_codes():
    tmp = xlrd.open_workbook('./FundCode.xlsx').sheet_by_index(0)
    codes = tmp.col_values(0)
    codes = [i.split(".")[0].split("!")[0].replace("F", '') for i in codes[1:]]
    for code in codes:
        if len(code) != 6:
            print('error', code)
    return codes


def check_make_directory(directory: str):
    if not os.path.exists(directory):
        os.mkdir(directory)


def save_pdf(title: str, fund_code: str, time: str, pdf_bytes: bytes):
    with open(f'./Reports/{fund_code}/{title}_{time}.pdf', 'wb') as f:
        f.write(pdf_bytes)


def save_txt(title: str, fund_code: str, time: str, txt_str: str):
    with open(f'./Reports/{fund_code}/{title}_{time}.txt', 'w', encoding='utf-8', errors='ignore') as f:
        f.write(txt_str)


def save_doc(title: str, fund_code: str, time: str, doc_bytes: bytes):
    with open(f'./Reports/{fund_code}/{title}_{time}.doc', 'wb') as f:
        f.write(doc_bytes)


def test_check():
    check_make_directory('./Reports/')
    codes = load_fund_codes()
    fund_code = codes[0]
    pre_list = get_list(fund_code)
    detail_info_list = extract_info(pre_list)
    print(len(detail_info_list))
    count = 0
    for code, title, time_stamp, ID, attach_type in detail_info_list:
        if attach_type == '0':
            path = f'./Reports/{code}/{title}_{time_stamp}.pdf'
            if not os.path.exists(path):
                count += 1
                print(code, title, time_stamp, ID, attach_type)
            print(count)


def check_content(content) -> bool:
    try:
        if type(content) == bytes:
            try:
                content.decode('utf-8')
                if 'The access control configuration prevents your request at this time' in content:
                    print('ERROR!')
                    return False
            except:
                print('true bytes')
                return True
        elif 'The access control configuration prevents your request at this time' in content:
            print('ERROR!')
            return False
        else:
            return True
    except Exception as e:
        with open('./Exceptions.txt', 'a') as f:
            f.write(str(e))
        print('ERROR!')
        return False

def start():
    check_make_directory('./Reports/')
    codes = load_fund_codes()
    for fund_code in codes:
        check_make_directory(f'./Reports/{fund_code}/')
        pre_list = get_list(fund_code)
        if not pre_list:
            continue
        detail_info_list = extract_info(pre_list)
        print(fund_code, detail_info_list)
        print(len(detail_info_list))
        count = 0
        for code, title, time_stamp, ID, attach_type in detail_info_list:
            title = title.replace("/", '').replace("\\", '').replace(":", '：')
            if os.path.exists(f'./Reports/{code}/{title}_{time_stamp}.pdf'):
                continue
            elif os.path.exists(f'./Reports/{code}/{title}_{time_stamp}.txt'):
                continue
            elif os.path.exists(f'./Reports/{code}/{title}_{time_stamp}.doc'):
                continue
            time.sleep(INTERVAL_INDEX)
            # pdf version
            if attach_type == '0':
                pdf_content = get_pdf(ID)
                if not pdf_content:
                    print(ID)
                    continue
                if not check_content(pdf_content):
                    time.sleep(300)
                    continue
                save_pdf(title, code, time_stamp, pdf_content)
            # txt version
            elif attach_type == '5':
                txt_content = get_txt(ID)
                if not txt_content:
                    print(ID)
                    continue
                if not check_content(txt_content):
                    time.sleep(300)
                    continue
                save_txt(title, code, time_stamp, txt_content)
            # doc version
            elif attach_type == '1':
                doc_content = get_doc(ID)
                if not doc_content:
                    print(ID)
                    continue
                if not check_content(doc_content):
                    time.sleep(300)
                    continue
                save_doc(title, code, time_stamp, doc_content)
            else:
                print(code, title, time_stamp, ID, attach_type)
            count += 1
            print(count)


def test():
    codes = load_fund_codes()
    fund_code = codes[0]
    li = get_list(fund_code)
    info_list = extract_info(li)
    ID = info_list[-1][3]
    print(generate_pdf_url(ID))
    pdf_content = get_pdf(ID)
    with open(f'./{info_list[0][3]}.pdf', 'wb') as f:
        f.write(pdf_content)
    print('test success, plz check test.pdf in the directory')

def download_single(codes):
    check_make_directory('./Reports/')
    for fund_code in codes:
        check_make_directory(f'./Reports/{fund_code}/')
        pre_list = get_list(fund_code)
        if not pre_list:
            continue
        detail_info_list = extract_info(pre_list)
        print(fund_code, detail_info_list)
        print(len(detail_info_list))
        count = 0
        for code, title, time_stamp, ID, attach_type in detail_info_list:
            title = title.replace("/", '').replace("\\", '').replace(":", '：')
            if os.path.exists(f'./Reports/{code}/{title}_{time_stamp}.pdf'):
                continue
            elif os.path.exists(f'./Reports/{code}/{title}_{time_stamp}.txt'):
                continue
            elif os.path.exists(f'./Reports/{code}/{title}_{time_stamp}.doc'):
                continue
            time.sleep(INTERVAL_INDEX)
            # pdf version
            if attach_type == '0':
                pdf_content = get_pdf(ID)
                if not pdf_content:
                    print(ID)
                    continue
                if not check_content(pdf_content):
                    time.sleep(300)
                    continue
                save_pdf(title, code, time_stamp, pdf_content)
            # txt version
            elif attach_type == '5':
                txt_content = get_txt(ID)
                if not txt_content:
                    print(ID)
                    continue
                if not check_content(txt_content):
                    time.sleep(300)
                    continue
                save_txt(title, code, time_stamp, txt_content)
            # doc version
            elif attach_type == '1':
                doc_content = get_doc(ID)
                if not doc_content:
                    print(ID)
                    continue
                if not check_content(doc_content):
                    time.sleep(300)
                    continue
                save_doc(title, code, time_stamp, doc_content)
            else:
                print(code, title, time_stamp, ID, attach_type)
            count += 1
            print(count)


if __name__ == '__main__':
    # test_check()
    start()
    download_single(['000082'])

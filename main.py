import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.ssl_ import create_urllib3_context
import json
import functools
import numpy as np
import pandas as pd
from collections import OrderedDict
from datetime import datetime
from dateutil import tz
from requests import exceptions
import argparse
import os
import xlsxwriter
from collections import namedtuple


class SSLContextAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        context = create_urllib3_context()
        kwargs['ssl_context'] = context
        context.load_default_certs() # this loads the OS defaults on Windows
        return super(SSLContextAdapter, self).init_poolmanager(*args, **kwargs)


base_url = "https://api.smartsurvey.io"
server = f"{base_url}/v1"
#api_token = "query string username" 
#api_token_secret = "query string password" 
headers = {'accept': 'application/json',
           'authorization': f'Basic add base-64 encoding string'} # generate at https://docs.smartsurvey.io/reference/surveys
https_proxy = 'https://1...:8080'


def get_surveys_from_file(filename):
    """Only used when developing not to hit the webservice"""
    with open(filename) as file:
        return json.load(file)


def save_surveys_to_file(filename, surveys):
    with open(filename, 'w') as file:
        json.dump(surveys, file)


def get_all_surveys():
    """Returns a dict: survey_id: survey_name"""
    page = 1
    returned_length = 100

    surveys = []

    while returned_length == 100:
        result = call(get_surveys_url_page(page))
        if result is None:
            print('Stopping.')
            return surveys
        returned_length = len(result)
        print(f'Returned {len(result)} surveys.')
        surveys += result
        print(f'Total count: {len(surveys)}.')
        page += 1

    # Only keep fields we're interested in, and truncate as necessary
    surveys = [
        {
            'id': str(s['id'])[:12],
            'title': s['title'][:255],
            'date_created': s['date_created'],
            'date_modified': s['date_modified'],
            'responses': s['responses'],
            'status': s['status'][:16]
        }
        for s in surveys
    ]

    return surveys


def get_surveys_url_page(page, page_size=100):
    return f"{server}/surveys?page_size={page_size}&page={page}" #dl


def get_survey_results(survey_id):
    page = 1
    returned_length = 100

    survey_results = []

    while returned_length == 100:
        result = call(get_survey_url(survey_id, page))
        if result is None:
            print('Stopping')
            return result
        returned_length = len(result)
        print(f'Returned {len(result)} survey results.')
        survey_results += result
        print(f'Total count: {len(survey_results)}.')
        page += 1

    return survey_results


def get_survey_results_from_file(results_folder, survey_id):
    filename = os.path.join(
        results_folder,
        f'survey_results_{survey_id}.json'
    )
    print(f'Reading file {filename}.')
    with open(filename) as file:
        return json.load(file)


def save_survey_results_to_file(results_folder, survey_results, survey_id):
    filename = os.path.join(
        results_folder,
        f'survey_results_{survey_id}.json'
    )
    with open(filename, 'w') as file:
        json.dump(survey_results, file)


def get_survey_url(survey_id, page, page_size=100):
    return f"{server}/surveys/{survey_id}/responses?include_labels=true&page_size={page_size}&page={page}" #dl


def call(url):
    keep_trying = True
    response = None
    while keep_trying:
        try:
            s = requests.Session()
            adapter = SSLContextAdapter()
            s.mount(base_url, adapter)
            response = s.get(url, headers=headers) #added headers=headers dl
            keep_trying = False
        except exceptions.SSLError:
            print('SSL error, please refresh https://api.smartsurvey.io')
            i = input('Contiue (y/n)?')
            if i == 'n':
                keep_trying = False

    return json.loads(response.content.decode('utf-8'))


def process_survey_responses(survey_responses, survey_id):
    """
    Takes the survey responses returned from the API, and returns two lists,
    one with individual response information, and one with all the answers.
    :param survey_responses:
    :param survey_id:
    :return:
    """
    responses = []
    answers = []

    for response in survey_responses:
        response_id = response['id']

        responses.append({
            'id': str(response_id)[:12],
            'survey_id': str(survey_id)[:12],
            'date_started': response['date_started'],
            'date_ended': response['date_ended'],
            'date_modified': response['date_modified'],
            'status': response['status'][:16]
        })

        # Unroll questions from pages
        questions = functools.reduce(lambda a, b: a + b['questions'],
                                     response['pages'],
                                     [])

        response_answers = [
            a
            for question in questions
            for a in extract_answers(question, response_id)
            if a is not None and a['answer'] != ''
        ]

        answers += response_answers

    return responses, answers


def fix_timezone(t):
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal()

    dt = datetime.strptime(t, '%Y-%m-%dT%H:%M:%SZ')
    dt.replace(tzinfo=from_zone)
    return datetime.strftime(dt.astimezone(to_zone), '%Y-%m-%d %H:%M:%S')


def extract_answers(question, response_id):
    """
    For each answer to the question, returns the 'extracted' tuple
    ((question_heading, question_subheading), answer).

    For example, the data:

        "id": 123456789,
        "title": "How would you rate:",
        "type": "matrix",
        "sub_type": "single",
        "number": 3,
        "position": 1,
        "answers": [
            {
                "id": 1022803343,
                "type": "matrix_row",
                "row_title": "The event overall?",
                "row_id": 66109417,
                "column_title": "4",
                "column_id": 66109426
            },
            {
                "id": 1022803344,
                "type": "matrix_row",
                "row_title": "- The content or theme of the event?",
                "row_id": 66109418,
                "column_title": "5 - Excellent",
                "column_id": 66109426
            },
            {
                "id": 1022803345,
                "type": "matrix_row",
                "row_title": "- The speakers / presenters / teachers / workshop leaders?",
                "row_id": 66109419,
                "column_title": "5 - Excellent",
                "column_id": 66109426
            },
            {
                "id": 1022803346,
                "type": "matrix_row",
                "row_title": "- The hosting of the event (welcome, facilities, venue, etc.)",
                "row_id": 66109420,
                "column_title": "4",
                "column_id": 66109425
            },
            {
                "id": 1022803347,
                "type": "matrix_row",
                "row_title": "- The event for value for money?",
                "row_id": 66109421,
                "column_title": "5 - Excellent",
                "column_id": 66109426
            }
        ]
    }

    returns
    [
        (("How would you rate:", "The event overall?"), "4"),
        (("How would you rate:", "- The content or theme of the event?"), "5 - Excellent"),
        (("How would you rate:", "- The speakers / presenters / teachers / workshop leaders?"), "5 - Excellent"),
        (("How would you rate:", "- The hosting of the event (welcome, facilities, venue, etc.)"), "4"),
        (("How would you rate:", "- The event for value for money?"), "5 - Excellent")
    ]

    :param question:
    :return:
    """
    return [
        extract_answer(question, answer, response_id)
        for answer in question['answers']
    ]


def extract_answer(question, answer, response_id):
    """
    For each question and answer API object, returns a tuple
    (list of question heading and possible sub heading, answer).

    :param question: A 'question' object, as returned by the API.
    :param answer: An 'answer' object, as returned by the API.
    :return: an object representing the question and answer.
    """
    def clean_string(s):
        """Removes unneccessary characters"""
        return (
            s.replace('\n', '')
            .replace('\r', '')
            .replace('&amp;', '&')
            .replace('&nbsp;', ' ')
            .strip()
        )

    def d(q, a, f):
        """
        Helper function that creates an object.
        :param q: List of questions
        :param a: Answer
        :return:
        """
        return {
            'id': str(answer['id'])[:12],
            'response_id': str(response_id)[:12],
            'question': clean_string(' '.join(q))[:255],
            'answer': clean_string(a)[:255],
            'free_text': f
        }

    # Special case for would you have paid:
    if answer['type'] == 'radio':
        return d([question['title']],
                 answer['choice_title'],
                 'N')
    elif answer['type'] == 'comment' or answer['type'] == 'other' or answer['type'] == 'text':
        return d([question['title'], answer['choice_title']],
                 answer.get('value', ''),
                 'Y')
    elif answer['type'] == 'matrix_row':
        return d([question['title'], answer['row_title']],
                 answer.get('column_title', ''),
                 'N')
    elif answer['type'] == 'checkbox':
        return d([question['title'], answer['choice_title']],
                 'Yes',
                 'N')
    elif answer['type'] == 'dropdown':
        return d([question['title']],
                 answer['choice_title'],
                 'N')

    else:
        return None


def surveys_to_df(surveys):
    columns_to_keep = [
        'id',
        'title',
        'date_created',
        'date_modified',
        'responses',
        'status'
    ]

    # Force dtype to string to prevent scrambling of data
    df = pd.DataFrame(surveys, dtype=str)[columns_to_keep]

    # Fix types
    df['date_created'] = df['date_created'].apply(fix_timezone)
    df['date_modified'] = df['date_modified'].apply(fix_timezone)
    df['responses'] = pd.to_numeric(df['responses'])

    return df


def responses_to_df(responses):
    # Force dtype to string to prevent scrambling of data
    df = pd.DataFrame(responses, dtype=str)

    # Fix types
    df['date_ended'] = df['date_ended'].apply(fix_timezone)
    df['date_modified'] = df['date_modified'].apply(fix_timezone)
    df['date_started'] = df['date_started'].apply(fix_timezone)

    return df


def answers_to_df(answers):
    # Force dtype to string to prevent scrambling of data
    df = pd.DataFrame(answers, dtype=str)

    return df[['id', 'response_id', 'question', 'answer', 'free_text']]


def read_surveys(args):
    if args.surveys_input_json is None:
        surveys = get_all_surveys()
    else:
        surveys = get_surveys_from_file(args.surveys_input_json)

    if args.surveys_output_json is not None:
        save_surveys_to_file(args.surveys_output_json, surveys)

    print(f'Read {len(surveys)} surveys.')

    surveys_df = surveys_to_df(surveys)
    print('Converted surveys to dataframe.')

    surveys_df.to_excel(args.surveys_output, sheet_name='Sheet 1', index=False)
    print(f'Saved surveys to {args.surveys_output}.')

    responses = []
    answers = []

    for survey in surveys:
        survey_id = survey['id']
        if args.survey_results_input_folder is None:
            survey_responses = get_survey_results(survey_id)
            if args.survey_results_output_folder is not None:
                save_survey_results_to_file(
                    args.survey_results_output_folder,
                    survey_responses,
                    survey_id
                )
        else:
            survey_responses = get_survey_results_from_file(
                args.survey_results_input_folder,
                survey_id
            )

        r, a = process_survey_responses(survey_responses, survey_id)
        responses += r
        answers += a

    # Create dataframes, force dtype to str to prevent scrambling of data
    responses_df = responses_to_df(responses)
    print('Converted responses to dataframe.')

    responses_df.to_excel(args.responses_output, sheet_name='Sheet 1', index=False)
    print(f'Saved responses to {args.responses_output}')

    answers_df = answers_to_df(answers)
    print('Converted answers to dataframe.')

    # Answers tends to be big, so we need a constant memory writer
    answers_writer = pd.ExcelWriter(args.answers_output, engine='xlsxwriter')
    answers_writer.book.use_zip64()
    answers_df.to_excel(answers_writer, sheet_name='Sheet 1', index=False)
    answers_writer.save()
    print(f'Saved ansers to {args.answers_output}.')


def main():
    args = _parse_args()
    read_surveys(args)


def _parse_args():
    parser = argparse.ArgumentParser(
        description='Calls SmartSurvey API, and produces long table '
                    'representation of the results.')

    parser.add_argument(
        '-s',
        '--surveys-input-json',
        help='location of surveys json'
             '(optional, if missing the API is called)',
        type=str,
        required=False
    )

    parser.add_argument(
        '-S',
        '--surveys-output-json',
        help='location to write surveys json'
             '(if missing, these are not written)',
        type=str,
        required=False
    )

    parser.add_argument(
        '-r',
        '--survey-results-input-folder',
        help='location to read individual survey results from,'
             'if missing the API is called',
        type=str,
        required=False
    )

    parser.add_argument(
        '-R',
        '--survey-results-output-folder',
        help='location to write individual survey results to'
             '(not applicable if --survey-results-input-folder is specified)',
        type=str,
        required=False
    )

    parser.add_argument(
        '-t',
        '--surveys-output',
        help='location to write surveys xlsx',
        type=str,
        required=True
    )

    parser.add_argument(
        '-o',
        '--responses-output',
        help='location to write responses xlsx',
        type=str,
        required=True
    )

    parser.add_argument(
        '-a',
        '--answers-output',
        help='location of answers xlsx file',
        type=str,
        required=True
    )

    return parser.parse_args()


if __name__ == "__main__":
    # execute only if run as a script
    main()

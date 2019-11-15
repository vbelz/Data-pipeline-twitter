import argparse

parser = argparse.ArgumentParser(description='ETL parsing')

parser.add_argument('--index_mongo', default=0, type=int)
parser.add_argument('--job_number', default=1, type=int)

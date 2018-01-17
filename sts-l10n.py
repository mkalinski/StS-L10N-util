#!/usr/bin/python3

"""
Converts the localization files back and forth between the JSON format and
a custom CSV format.

The CSV format is meant to make it easier for multiple people to edit the file
in some sort of collaborative spreadsheet.

The custom CSV format is not a proper line-oriented format, but instead uses
data written in a column, with some of the cells marked as keys, and all others
as values. They correspond to keys and values from the JSON.

The format uses two or more columns, with the following meaning, separated by
tabs:

<type> <row> ...

Where <type> denotes the type of the row, and can be one of the values:

- Number: the row is a key with an associated array value. As many value
rows must follow this row as is denoted by this number. The number can
also be 0 (no values following, an empty array) or 1 (one value following, as
with scalar, but when converting to JSON it will result in a one-element array
instead of just a string).

- Dash ("-"): the row is a key with an associated scalar value. Exactly one
value row must follow this key.

- Empty (nothing in cell): the row is a value row for the nearest preceding key
row.

Key row names are created by joining nested JSON object keys with the "::"
separator. They're converted back into nested objects, with the value rows
denoting the innermost array or string value.

For example, if we have a JSON record in the format of:

{
    "Cards": {
        "Strike": {
            "NAME": "Strike"
        },
        ...
    }
}

It will be converted to:

-   Cards::Strike::NAME
    Strike
... ...

The key and value rows together are referred to as a "record".

There can be only one key column, but multiple value columns. It's possible to
choose from which column to select values when converting back to JSON.
"""

from argparse import ArgumentParser
from collections.abc import Mapping, Sequence
from collections import OrderedDict, namedtuple
import csv
from itertools import chain
import json
import sys


CSV_DIALECT = 'excel-tab'

PureRecord = namedtuple('PureRecord', ('name_parts', 'value'))


def main():
    ap = make_argument_parser()
    av = ap.parse_args()

    # Must be checked manually because of bug
    # https://stackoverflow.com/a/22994500
    if not hasattr(av, 'cmd'):
        ap.error('choose one of the commands')

    infile = FileOrStream(av.input_file,
                          sys.stdin,
                          {'mode': 'r', 'newline': av.newline})

    outfile = FileOrStream(av.output_file,
                           sys.stdout,
                           {'mode': 'w', 'newline': av.newline})

    with infile as instream, outfile as outstream:
        av.cmd(instream, outstream, av)


def make_argument_parser():
    ap = ArgumentParser(description=__doc__)
    ap.add_argument('-i', '--input-file')
    ap.add_argument('-o', '--output-file')

    aps = ap.add_subparsers()

    ap_csv = aps.add_parser('json2csv')
    ap_csv.set_defaults(cmd=convert_json_to_csv, newline='')

    ap_json = aps.add_parser('csv2json')
    ap_json.add_argument('-c', '--select-column',
                         type=column_opt_type, default=1)
    ap_json.add_argument('-s', '--skip-rows', type=skip_opt_type, default=0)
    ap_json.set_defaults(cmd=convert_csv_to_json, newline=None)

    return ap


def column_opt_type(value):
    intval = int(value)

    if intval < 1:
        raise ValueError('Selected column must be >= 1 (is {})'.format(intval))

    return intval


def skip_opt_type(value):
    intval = int(value)

    if intval < 0:
        raise ValueError('Skipped rows must be >= 0 (is {})'.format(intval))

    return intval


def convert_json_to_csv(in_stream, out_stream, argv):
    records = read_records_from_json(in_stream)
    csv_writer = csv.writer(out_stream, CSV_DIALECT)
    write_records_to_csv(csv_writer, records)


def convert_csv_to_json(in_stream, out_stream, argv):
    csv_reader = csv.reader(in_stream, CSV_DIALECT)
    records = read_records_from_csv(csv_reader,
                                    argv.select_column,
                                    argv.skip_rows)
    write_records_to_json(out_stream, records)


class RecordNameParts(tuple):
    """
    Tuple representing multi-part name of a record key.

    For example, ("Cards", "Strike", "NAME").

    Can be converted to and from the string representation used in the CSV
    format.

    For simplicity, the addition operator is overridden so that if a string is
    added to an instance of this tuple, the result is a new tuple with that
    string as the last name part.
    """

    __slots__ = ()

    SEP = '::'

    @classmethod
    def from_str(cls, name):
        return cls(name.split(cls.SEP))

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, super().__repr__())

    def __str__(self):
        return self.SEP.join(self)

    def __add__(self, other):
        add_val = (other,) if isinstance(other, str) else other
        return RecordNameParts(chain(self, add_val))


class CSVRecord(namedtuple('CSVRecord', ('type', 'key', 'values'))):
    """
    Represents a record read from CSV, or ready to be written to CSV. Should be
    converted from and to PureRecord when processing inside the program.

    It contains some data specific to the CSV format that is not necessary in
    natural JSON-like dicts.
    """

    __slots__ = ()

    TYPE_SCALAR = '-'
    VALUE_ROW_TYPE = ''

    @classmethod
    def get_number_of_values_for_type(cls, typesig):
        return 1 if typesig == cls.TYPE_SCALAR else int(typesig)

    @classmethod
    def from_pure_record(cls, record):
        if isinstance(record.value, str):
            typesig = cls.TYPE_SCALAR
            values = [record.value]
        elif isinstance(record.value, Sequence):
            typesig = len(record.value)
            values = list(record.value)
        else:
            raise UnexpectedRecordValueException(
                '{!r} (of {})'.format(record.value, record.name_parts)
            )

        return cls(str(typesig), str(record.name_parts), values)

    def to_pure_record(self):
        value = (self.values[0]
                 if self.type == self.TYPE_SCALAR
                 else self.values)
        return PureRecord(RecordNameParts.from_str(self.key), value)

    def verify(self):
        if self.type == self.TYPE_SCALAR:
            self.__verify_scalar()
        else:
            self.__verify_array()

    def write_to_csv(self, csv_writer):
        csv_writer.writerow((self.type, self.key))

        for value in self.values:
            csv_writer.writerow((self.VALUE_ROW_TYPE, value))

    def __verify_scalar(self):
        if len(self.values) != 1:
            raise CSVRecordVerificationFailure(
                '{!r}: Number of values for scalar record not exactly 1'
                .format(self)
            )

    def __verify_array(self):
        declared_len = self.__verify_length_sign()

        if len(self.values) != declared_len:
            raise CSVRecordVerificationFailure(
                '{!r}: Array length declared in type ({}) '
                'not corresponding to the actual length of values ({})'
                .format(self, declared_len, len(self.values))
            )

    def __verify_length_sign(self):
        try:
            return int(self.type)
        except ValueError:
            raise CSVRecordVerificationFailure(
                '{!r}: Invalid type sign ({})'.format(self, self.type)
            )


class CSVRecordReader:

    def __init__(self, rows_iter, values_column=1):
        self._rows_iter = rows_iter
        self.__verify_values_column(values_column)
        self._val_col = values_column

    def read_one_record(self):
        typesig, key = self._read_key_row()
        rows_to_read = CSVRecord.get_number_of_values_for_type(typesig)
        values = self._read_value_rows(rows_to_read)

        rec = CSVRecord(typesig, key, values)
        rec.verify()
        return rec

    def read_all_records(self):
        records = []

        while True:
            try:
                records.append(self.read_one_record())
            except CSVEndOfRowsException:
                break

        return records

    def _read_key_row(self):
        row = self._read_one_row()
        self._verify_key_row(row)
        return row

    def _read_one_row(self):
        try:
            return next(self._rows_iter)
        except StopIteration:
            raise CSVEndOfRowsException()

    def _read_value_rows(self, number):
        read_values = []

        if number > 0:
            read_values.extend(self._gen_values(number))
            self._verify_values_number(read_values, number)

        return read_values

    def _gen_values(self, number_of_rows):
        for rnum, row in enumerate(self._rows_iter, 1):
            value = self._get_row_value(row)
            yield value

            if rnum >= number_of_rows:
                break

    def _get_row_value(self, value_row):
        if (len(value_row) < max(2, self._val_col + 1) or
                value_row[0] != CSVRecord.VALUE_ROW_TYPE):
            raise CSVFormatException('Invalid value row: ' + repr(value_row))

        return value_row[self._val_col]

    @staticmethod
    def _verify_key_row(key_row):
        if len(key_row) != 2:
            raise CSVFormatException('Invalid key row: ' + repr(key_row))

    @staticmethod
    def _verify_values_number(values, number):
        if len(values) != number:
            raise CSVFormatException('Excepted to read {} rows, but read {}: '
                                     '{!r}'
                                     .format(number, len(values), values))

    @staticmethod
    def __verify_values_column(values_column):
        if values_column < 1:
            raise ValueError('values_column must be >= 1 (is {})'
                             .format(values_column))


class OutputJSONDict(OrderedDict):

    __slots__ = ()

    def __missing__(self, key):
        newd = OutputJSONDict()
        self[key] = newd
        return newd

    def include_record(self, record):
        record_target = self.__get_nested_dict(record.name_parts[:-1])
        record_target[record.name_parts[-1]] = record.value

    def __get_nested_dict(self, key_seq):
        target = self

        for key in key_seq:
            target = target[key]

        return target


class FileOrStream:

    def __init__(self, filename, stream, open_options=None):
        self._fname = filename
        self._stream = stream
        self._opts = open_options or {}

    def __enter__(self):
        if self._fname:
            self._stream = open(self._fname, **self._opts)

        return self._stream

    def __exit__(self, exc_type, exc_value, exc_tb):
        if self._fname:
            self._stream.close()


class UnexpectedRecordValueException(Exception):
    pass


class CSVRecordVerificationFailure(Exception):
    pass


class CSVFormatException(Exception):
    pass


class CSVEndOfRowsException(Exception):
    pass


def write_records_to_json(json_file, records):
    out_dict = OutputJSONDict()

    for record in records:
        out_dict.include_record(record)

    json.dump(out_dict, json_file, indent=2)


def read_records_from_json(json_file):
    json_dict = json.load(json_file, object_pairs_hook=OrderedDict)
    return collect_records_from_json(json_dict)


def collect_records_from_json(json_dict, parent_name_parts=RecordNameParts()):
    records = []

    for key, value in json_dict.items():
        name_parts = parent_name_parts + key

        if isinstance(value, Sequence):
            records.append(PureRecord(name_parts, value))
        elif isinstance(value, Mapping):
            records.extend(collect_records_from_json(value, name_parts))

    return records


def write_records_to_csv(csv_writer, records):
    for record in records:
        csv_rec = CSVRecord.from_pure_record(record)
        csv_rec.write_to_csv(csv_writer)


def read_records_from_csv(csv_reader, values_column=1, skipped_rows=0):
    row_iter = iter(csv_reader)
    skip_iter_items(row_iter, skipped_rows)

    record_reader = CSVRecordReader(row_iter, values_column)
    return [csv_rec.to_pure_record()
            for csv_rec in record_reader.read_all_records()]


def skip_iter_items(iterator, number_of_skipped):
    for _ in range(number_of_skipped):
        next(iterator, None)


if __name__ == '__main__':
    main()

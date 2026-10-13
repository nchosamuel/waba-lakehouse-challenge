import csv
import io
import unittest
from unittest.mock import Mock
from waba.full_references import compatible_existing
from waba.reference_scale import baseline


class FullReferenceSafetyTests(unittest.TestCase):
    def check_data(self, changed=False):
        rows = [dict(row) for row in baseline('CI')['products']]
        if changed:
            rows[0]['product_name'] = 'Custom user value'
        stream = io.StringIO()
        writer = csv.DictWriter(stream,fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
        s3 = Mock()
        s3.head_object.return_value = {'Metadata':{}}
        body = Mock()
        body.iter_lines.return_value = stream.getvalue().encode().splitlines(keepends=True)
        s3.get_object.return_value = {'Body':body}
        return s3,body

    def test_existing_rows_can_be_extended(self):
        s3,body = self.check_data()
        self.assertFalse(compatible_existing(s3,'CI/references/products.csv','products','CI',{'sha256':'new'}))
        body.close.assert_called_once()

    def test_conflicting_value_is_preserved(self):
        s3,body = self.check_data(changed=True)
        with self.assertRaises(ValueError):
            compatible_existing(s3,'CI/references/products.csv','products','CI',{'sha256':'new'})
        s3.upload_file.assert_not_called()
        body.close.assert_called_once()

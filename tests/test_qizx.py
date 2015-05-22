# -*- coding: utf-8 -*-
"""Qizx RESTful API client unit tests.

This code is part of the Qizx application components
Copyright (c) 2015 Michael Paddon

For conditions of use, see the accompanying license files.
"""

import qizx
import unittest


# for python 2 compatibility
def _unicode(x):
    return x.decode("utf-8") if hasattr(x, "decode") else x


class ApiTest(unittest.TestCase):
    """RESTful API unit tests."""

    _library = "test_qizx_py"
    _hello_xml = "<hello-world>Japanese 今日は世界！ Chinese 你好 Korean 안녕하세요 Czech Dvořák Greek γεια Hungarian Helló Russian привет Spanish ¡Hola! Swedish hallå</hello-world>\n"

    def setUp(self):
        self._client = qizx.Client()

    def tearDown(self):
        self._client.close()

    def test_00_info(self):
        info = self._client.info()
        self.assertEqual(info["product-name"], "Qizx")

    def test_01_mklib(self):
        self._client.dellib(self._library)
        self._client.mklib(self._library)

    def test_02_mkcol(self):
        self._client.mkcol("/test", library=self._library)

    def test_03_put(self):
        self._client.put([("/test/hello.xml", self._hello_xml)],
                         library=self._library)

    def test_04_get(self):
        text = self._client.get("/test", library=self._library)
        self.assertIn("/test/hello.xml", text.splitlines())

    def test_05_get(self):
        text = self._client.get("/test/hello.xml", library=self._library)
        self.assertTrue(text.find(_unicode("今日は世界！")) != -1)

    def test_06_eval_japanese(self):
        items = self._client.eval('/*[. ftcontains "今日は世界！"]',
                                  format="items", library=self._library)
        print(items)
        self.assertGreater(len(items), 0)
        self.assertTrue(items[0].find(_unicode("今日は世界！")) != -1)

    def test_06_eval_chinese(self):
        items = self._client.eval('/*[. ftcontains "你好"]',
                                  format="items", library=self._library)
        print(items)
        self.assertGreater(len(items), 0)
        self.assertTrue(items[0].find(_unicode("你好")) != -1)

    def test_06_eval_korean(self):
        items = self._client.eval('/*[. ftcontains "안녕하세요"]',
                                  format="items", library=self._library)
        print(items)
        self.assertGreater(len(items), 0)
        self.assertTrue(items[0].find(_unicode("안녕하세요")) != -1)

    def test_06_eval_czech(self):
        items = self._client.eval('/*[. ftcontains "Dvořák"]',
                                  format="items", library=self._library)
        print(items)
        self.assertGreater(len(items), 0)
        self.assertTrue(items[0].find(_unicode("Dvořák")) != -1)

    def test_06_eval_greek(self):
        items = self._client.eval('/*[. ftcontains "γεια"]',
                                  format="items", library=self._library)
        print(items)
        self.assertGreater(len(items), 0)
        self.assertTrue(items[0].find(_unicode("γεια")) != -1)

    def test_06_eval_hungarian(self):
        items = self._client.eval('/*[. ftcontains "Helló"]',
                                  format="items", library=self._library)
        print(items)
        self.assertGreater(len(items), 0)
        self.assertTrue(items[0].find(_unicode("Helló")) != -1)

    def test_06_eval_russian(self):
        items = self._client.eval('/*[. ftcontains "привет"]',
                                  format="items", library=self._library)
        print(items)
        self.assertGreater(len(items), 0)
        self.assertTrue(items[0].find(_unicode("привет")) != -1)

    def test_06_eval_spanish(self):
        items = self._client.eval('/*[. ftcontains "¡Hola!"]',
                                  format="items", library=self._library)
        print(items)
        self.assertGreater(len(items), 0)
        self.assertTrue(items[0].find(_unicode("¡Hola!")) != -1)

    def test_06_eval_swedish(self):
        items = self._client.eval('/*[. ftcontains "hallå"]',
                                  format="items", library=self._library)
        print(items)
        self.assertGreater(len(items), 0)
        self.assertTrue(items[0].find(_unicode("hallå")) != -1)

    def test_07_copy(self):
        self._client.copy("/test/hello.xml", "/test/hello_copy.xml",
                          library=self._library)

    def test_08_move(self):
        self._client.move("/test/hello_copy.xml", "/test/world.xml",
                          library=self._library)

    def test_09_delete(self):
        self._client.delete("/test/world.xml",
                            library=self._library)

    def test_10_setprop(self):
        self._client.setprop("/test/hello.xml", [("hello", "world")],
                             library=self._library)

    def test_11_getprop(self):
        properties = self._client.getprop("/test/hello.xml",
                                          library=self._library)
        self.assertEqual(properties["/test/hello.xml"]["hello"], "world")

    def test_12_queryprop(self):
        properties = self._client.queryprop('path="/test/hello.xml"',
                                            ["hello"], library=self._library)
        self.assertEqual(properties["/test/hello.xml"]["hello"], "world")

    def test_13_listlib(self):
        libraries = self._client.listlib()
        self.assertIn(self._library, libraries)

    def test_14_server(self):
        self.assertTrue(self._client.server("status"))

    def test_15_reindex(self):
        id = self._client.reindex(self._library)
        self._client.wait(id, poll=1)

    def test_16_optimize(self):
        id = self._client.optimize(self._library)
        self._client.wait(id, poll=1)

    def test_17_getstats(self):
        stats = self._client.getstats()
        self.assertTrue("Description" in stats[0])

    def test_18_listtasks(self):
        self._client.listtasks()

    def test_19_listqueries(self):
        self._client.listqueries()

    def test_99_dellib(self):
        self._client.dellib(self._library)

if __name__ == '__main__':
    unittest.main()

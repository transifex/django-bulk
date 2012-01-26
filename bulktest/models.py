from django.db import models


class TestModelA(models.Model):
    a = models.CharField(max_length=200)
    b = models.IntegerField()
    c = models.IntegerField()


class TestModelPreSave(models.Model):
    """Model that defines the presave method."""

    a = models.IntegerField()

    def presave(self):
        self.a = 5

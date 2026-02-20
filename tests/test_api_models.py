"""Tests for BRREG API response model parsing."""

from __future__ import annotations

from brreg_regnskap.api.models import Enhet, EnhetUpdate, ManifestRecord, Regnskap


class TestEnhetModel:
    def test_parse_full_entity(self, enhet_json: dict) -> None:
        e = Enhet.model_validate(enhet_json)
        assert e.organisasjonsnummer == "964118191"
        assert e.navn == "MOWI ASA"
        assert e.sisteInnsendteAarsregnskap == "2024"
        assert e.antallAnsatte == 2530
        assert e.konkurs is False
        assert e.erIKonsern is True

    def test_naeringskode_parsing(self, enhet_json: dict) -> None:
        e = Enhet.model_validate(enhet_json)
        assert e.naeringskode1 is not None
        assert e.naeringskode1.kode == "03.211"
        assert e.naeringskode2 is not None
        assert e.naeringskode2.kode == "03.222"

    def test_organisasjonsform_parsing(self, enhet_json: dict) -> None:
        e = Enhet.model_validate(enhet_json)
        assert e.organisasjonsform is not None
        assert e.organisasjonsform.kode == "ASA"

    def test_minimal_entity(self) -> None:
        e = Enhet.model_validate({"organisasjonsnummer": "123456789"})
        assert e.organisasjonsnummer == "123456789"
        assert e.navn is None
        assert e.sisteInnsendteAarsregnskap is None

    def test_extra_fields_allowed(self, enhet_json: dict) -> None:
        enhet_json["unknownField"] = "should not raise"
        e = Enhet.model_validate(enhet_json)
        assert e.organisasjonsnummer == "964118191"


class TestRegnskapModel:
    def test_parse_regnskap(self, regnskap_json: list) -> None:
        r = Regnskap.model_validate(regnskap_json[0])
        assert r.journalnr == "2025741982"
        assert r.regnskapstype == "SELSKAP"
        assert r.valuta == "EUR"

    def test_virksomhet_parsing(self, regnskap_json: list) -> None:
        r = Regnskap.model_validate(regnskap_json[0])
        assert r.virksomhet is not None
        assert r.virksomhet.organisasjonsnummer == "964118191"
        assert r.virksomhet.morselskap is True

    def test_regnskapsperiode_parsing(self, regnskap_json: list) -> None:
        r = Regnskap.model_validate(regnskap_json[0])
        assert r.regnskapsperiode is not None
        assert r.regnskapsperiode.fraDato == "2024-01-01"
        assert r.regnskapsperiode.tilDato == "2024-12-31"

    def test_nested_financial_data_preserved(self, regnskap_json: list) -> None:
        r = Regnskap.model_validate(regnskap_json[0])
        assert r.resultatregnskapResultat is not None
        assert r.resultatregnskapResultat["aarsresultat"] == 194000000.00
        assert r.eiendeler is not None
        assert r.eiendeler["sumEiendeler"] == 6440000000.00


class TestEnhetUpdate:
    def test_parse_update(self) -> None:
        u = EnhetUpdate.model_validate(
            {
                "oppdateringsid": 12345,
                "dato": "2025-02-01",
                "organisasjonsnummer": "964118191",
                "endringstype": "Endring",
                "endringer": [
                    {"op": "replace", "path": "/sisteInnsendteAarsregnskap", "value": "2024"}
                ],
            }
        )
        assert u.oppdateringsid == 12345
        assert u.endringstype == "Endring"
        assert len(u.endringer) == 1

    def test_update_without_changes(self) -> None:
        u = EnhetUpdate.model_validate(
            {
                "oppdateringsid": 1,
                "dato": "2025-01-01",
                "organisasjonsnummer": "123456789",
                "endringstype": "Ny",
            }
        )
        assert u.endringer is None


class TestManifestRecord:
    def test_default_values(self) -> None:
        r = ManifestRecord(orgnr="964118191", year=2024, download_timestamp="2025-01-01T00:00:00Z")
        assert r.status == "pending"
        assert r.is_correction is False
        assert r.file_hash is None

    def test_full_record(self) -> None:
        r = ManifestRecord(
            orgnr="964118191",
            year=2024,
            download_timestamp="2025-01-01T00:00:00Z",
            file_hash="abc123",
            json_path="regnskap/964118191/regnskap_2024.json",
            pdf_path="regnskap/964118191/aarsregnskap_2024.pdf",
            file_size_bytes=1024,
            is_correction=False,
            journalnr="2025741982",
            source_url="https://data.brreg.no/regnskapsregisteret/regnskap/964118191",
            status="success",
        )
        assert r.journalnr == "2025741982"
        assert r.status == "success"

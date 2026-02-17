"""Pydantic models for BRREG API responses.

These models parse the JSON returned by Enhetsregisteret and Regnskapsregisteret APIs.
Fields use Optional where BRREG may omit them. Norwegian field names are preserved
to match the API exactly — no renaming.

Implementation notes for agents:
    - Parse the full entity response from /api/enheter/{orgnr}
    - Parse the regnskap response from /regnskapsregisteret/regnskap/{orgnr}
    - The regnskap endpoint returns XML by default; request JSON via Accept header
    - The `journalnr` field is critical for correction detection
    - Add additional nested models as needed when implementing (e.g., Adresse, Naeringskode)
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class Naeringskode(BaseModel):
    model_config = ConfigDict(extra="allow")
    kode: str
    beskrivelse: str | None = None


class Organisasjonsform(BaseModel):
    model_config = ConfigDict(extra="allow")
    kode: str
    beskrivelse: str | None = None


class Adresse(BaseModel):
    model_config = ConfigDict(extra="allow")
    land: str | None = None
    landkode: str | None = None
    postnummer: str | None = None
    poststed: str | None = None
    adresse: list[str] | None = None
    kommune: str | None = None
    kommunenummer: str | None = None


class Enhet(BaseModel):
    """A single entity from Enhetsregisteret.

    The field `sisteInnsendteAarsregnskap` is the trigger for downloading regnskap.
    When this field changes (detected via the updates API), the entity needs re-processing.
    """

    model_config = ConfigDict(extra="allow")

    organisasjonsnummer: str
    navn: str | None = None
    organisasjonsform: Organisasjonsform | None = None
    hjemmeside: str | None = None
    postadresse: Adresse | None = None
    forretningsadresse: Adresse | None = None
    registreringsdatoEnhetsregisteret: str | None = None
    registrertIMvaregisteret: bool | None = None
    naeringskode1: Naeringskode | None = None
    naeringskode2: Naeringskode | None = None
    naeringskode3: Naeringskode | None = None
    antallAnsatte: int | None = None
    harRegistrertAntallAnsatte: bool | None = None
    stiftelsesdato: str | None = None
    registrertIForetaksregisteret: bool | None = None
    sisteInnsendteAarsregnskap: str | None = None
    konkurs: bool | None = None
    underAvvikling: bool | None = None
    underTvangsavviklingEllerTvangsopplosning: bool | None = None
    erIKonsern: bool | None = None


class EnhetUpdate(BaseModel):
    """A change event from /api/oppdateringer/enheter.

    The `oppdateringsid` is a monotonically increasing cursor used for pagination.
    When `includeChanges=true`, the `endringer` field contains JSON Patch operations
    showing which fields changed. Filter for changes to `sisteInnsendteAarsregnskap`.
    """

    model_config = ConfigDict(extra="allow")

    oppdateringsid: int
    dato: str
    organisasjonsnummer: str
    endringstype: str  # Ny, Endring, Sletting, Fjernet
    endringer: list[dict] | None = None  # JSON Patch operations when includeChanges=true


class Regnskapsperiode(BaseModel):
    model_config = ConfigDict(extra="allow")
    fraDato: str | None = None
    tilDato: str | None = None


class RegnskapVirksomhet(BaseModel):
    model_config = ConfigDict(extra="allow")
    organisasjonsnummer: str | None = None
    organisasjonsform: str | None = None
    morselskap: bool | None = None


class Regnskap(BaseModel):
    """A single regnskap record from Regnskapsregisteret.

    The `journalnr` field identifies the specific submission. When a company submits
    a corrected regnskap, a new record appears with a different journalnr for the
    same regnskapsperiode. Comparing journalnr against the manifest detects corrections.

    The nested financial fields (resultatregnskapResultat, eiendeler, egenkapitalGjeld)
    contain the actual accounting figures. These are stored as dicts here to avoid
    deep nesting — the raw JSON is preserved in storage for full fidelity.
    """

    model_config = ConfigDict(extra="allow")

    id: int | None = None
    journalnr: str | None = None
    regnskapstype: str | None = None
    virksomhet: RegnskapVirksomhet | None = None
    regnskapsperiode: Regnskapsperiode | None = None
    valuta: str | None = None
    avviklingsregnskap: bool | None = None
    oppstillingsplan: str | None = None
    resultatregnskapResultat: dict | None = None
    eiendeler: dict | None = None
    egenkapitalGjeld: dict | None = None
    revisjon: dict | None = None
    regnkapsprinsipper: dict | None = None


class ManifestRecord(BaseModel):
    """A row in the Parquet manifest tracking a downloaded file pair (JSON + PDF).

    This model is used for creating/reading manifest entries. The Parquet schema
    in manifest.py must match these fields exactly.
    """

    orgnr: str
    year: int
    download_timestamp: str  # ISO 8601 UTC
    file_hash: str | None = None  # SHA-256 of JSON content
    json_path: str | None = None
    pdf_path: str | None = None
    file_size_bytes: int | None = None
    is_correction: bool = False
    journalnr: str | None = None
    source_url: str | None = None
    status: str = "pending"  # pending, success, failed, pdf_missing

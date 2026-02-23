from __future__ import annotations

from datetime import date
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# --- Enums ---

class Organisasjonsform(str, Enum):
    aksjeselskap = "Aksjeselskap"
    allmennaksjeselskap = "Allmennaksjeselskap"
    enkeltpersonforetak = "Enkeltpersonforetak"
    ans = "ANS"
    da = "DA"
    nuf = "NUF"
    other = "other"


class Revisjonskonklusjon(str, Enum):
    uten_forbehold = "uten forbehold"
    med_forbehold = "med forbehold"
    negativ = "negativ"
    kan_ikke_uttale_seg = "kan ikke uttale seg"
    annet = "annet"


# --- Reusable two-year amount pair ---

class AarsPar(BaseModel):
    """Current-year / prior-year amount pair. All amounts in NOK."""
    currentYear: Optional[int] = None
    priorYear: Optional[int] = None


# --- Section: generellInformasjon ---

class Forretningsadresse(BaseModel):
    gateadresse: Optional[str] = None
    postnummer: Optional[str] = None
    poststed: Optional[str] = None


class GenerellInformasjon(BaseModel):
    """General company and report information from the cover page and header sections."""
    foretaksnavn: Optional[str] = Field(None, description="Legal name of the company.")
    organisasjonsnummer: Optional[str] = Field(None, description="Norwegian organization number, typically 9 digits.", examples=["964 026 521"])
    organisasjonsform: Optional[Organisasjonsform] = None
    forretningsadresse: Optional[Forretningsadresse] = None
    regnskapsaarStart: Optional[date] = Field(None, description="Start date of the fiscal year.")
    regnskapsaarSlutt: Optional[date] = Field(None, description="End date of the fiscal year.")
    datoForFastsettelse: Optional[date] = Field(None, description="Date when the annual report was approved.")
    morselskapIKonsern: Optional[bool] = Field(None, description="Whether the company is a parent company in a group.")
    reglerForSmaaForetakBenyttet: Optional[bool] = Field(None, description="Whether simplified rules for small companies were applied.")
    regnskapsregler: Optional[str] = Field(None, description="Accounting standard used (e.g., Regnskapslovens alminnelige regler, IFRS).")
    utarbeidetAvEksternRegnskapsforer: Optional[bool] = Field(None, description="Whether prepared by an external authorized accountant.")
    bekreftetAv: Optional[str] = Field(None, description="Name of the person who confirmed/approved the annual report.")


# --- Section: resultatregnskap ---

class Driftsinntekter(BaseModel):
    salgsinntekt: Optional[AarsPar] = None
    annenDriftsinntekt: Optional[AarsPar] = None
    sumDriftsinntekter: Optional[AarsPar] = None


class Driftskostnader(BaseModel):
    varekostnad: Optional[AarsPar] = None
    lonnskostnad: Optional[AarsPar] = None
    avskrivning: Optional[AarsPar] = None
    nedskrivning: Optional[AarsPar] = None
    annenDriftskostnad: Optional[AarsPar] = None
    sumDriftskostnader: Optional[AarsPar] = None


class Finansposter(BaseModel):
    renteinntekt: Optional[AarsPar] = None
    annenFinansinntekt: Optional[AarsPar] = None
    sumFinansinntekter: Optional[AarsPar] = None
    rentekostnad: Optional[AarsPar] = None
    annenFinanskostnad: Optional[AarsPar] = None
    sumFinanskostnader: Optional[AarsPar] = None
    nettoFinans: Optional[AarsPar] = None


class OverforingerOgDisponeringer(BaseModel):
    avgittKonsernbidrag: Optional[AarsPar] = None
    avsattTilAnnenEgenkapital: Optional[AarsPar] = None
    sumOverforinger: Optional[AarsPar] = None


class Resultatregnskap(BaseModel):
    """Income statement (resultatregnskap) with current and prior year figures. All amounts in NOK."""
    inneværendeAar: Optional[int] = Field(None, description="The current reporting year.")
    forrigeAar: Optional[int] = Field(None, description="The prior comparison year.")
    driftsinntekter: Optional[Driftsinntekter] = None
    driftskostnader: Optional[Driftskostnader] = None
    driftsresultat: Optional[AarsPar] = None
    finansposter: Optional[Finansposter] = None
    resultatForSkattekostnad: Optional[AarsPar] = None
    skattekostnad: Optional[AarsPar] = None
    resultatEtterSkattekostnad: Optional[AarsPar] = None
    aarsresultat: Optional[AarsPar] = None
    totalresultat: Optional[AarsPar] = None
    overforingerOgDisponeringer: Optional[OverforingerOgDisponeringer] = None


# --- Section: balanse ---

class UtsattSkattefordel(BaseModel):
    utsattSkattefordel: Optional[AarsPar] = None
    sumImmaterielleEiendeler: Optional[AarsPar] = None


class VarigeDriftsmidler(BaseModel):
    driftslosoreInventarOaUtstyr: Optional[AarsPar] = None
    sumVarigeDriftsmidler: Optional[AarsPar] = None


class Anleggsmidler(BaseModel):
    immaterielleEiendeler: Optional[UtsattSkattefordel] = None
    varigeDriftsmidler: Optional[VarigeDriftsmidler] = None
    sumAnleggsmidler: Optional[AarsPar] = None


class Fordringer(BaseModel):
    kundefordringer: Optional[AarsPar] = None
    andreKortsiktigeFordringer: Optional[AarsPar] = None
    sumFordringer: Optional[AarsPar] = None


class Omlopsmidler(BaseModel):
    varer: Optional[AarsPar] = None
    fordringer: Optional[Fordringer] = None
    bankinnskuddKontanterOgLignende: Optional[AarsPar] = None
    sumOmlopsmidler: Optional[AarsPar] = None


class Eiendeler(BaseModel):
    anleggsmidler: Optional[Anleggsmidler] = None
    omlopsmidler: Optional[Omlopsmidler] = None
    sumEiendeler: Optional[AarsPar] = None


class InnskuttEgenkapital(BaseModel):
    aksjekapital: Optional[AarsPar] = None
    sumInnskuttEgenkapital: Optional[AarsPar] = None


class OpptjentEgenkapital(BaseModel):
    annenEgenkapital: Optional[AarsPar] = None
    sumOpptjentEgenkapital: Optional[AarsPar] = None


class Egenkapital(BaseModel):
    innskuttEgenkapital: Optional[InnskuttEgenkapital] = None
    opptjentEgenkapital: Optional[OpptjentEgenkapital] = None
    sumEgenkapital: Optional[AarsPar] = None


class KortsiktigGjeld(BaseModel):
    leverandorgjeld: Optional[AarsPar] = None
    betalbarSkatt: Optional[AarsPar] = None
    skyldigOffentligeAvgifter: Optional[AarsPar] = None
    konserngjeld: Optional[AarsPar] = None
    annenKortsiktigGjeld: Optional[AarsPar] = None
    sumKortsiktigGjeld: Optional[AarsPar] = None


class Gjeld(BaseModel):
    langsiktigGjeld: Optional[AarsPar] = Field(None, description="Sum of long-term liabilities. Use sumLangsiktigGjeld for the total.")
    sumLangsiktigGjeld: Optional[AarsPar] = None
    kortsiktigGjeld: Optional[KortsiktigGjeld] = None
    sumGjeld: Optional[AarsPar] = None


class EgenkapitalOgGjeld(BaseModel):
    egenkapital: Optional[Egenkapital] = None
    gjeld: Optional[Gjeld] = None
    sumEgenkapitalOgGjeld: Optional[AarsPar] = None


class Balanse(BaseModel):
    """Balance sheet (balanse) with current and prior year figures. All amounts in NOK."""
    eiendeler: Optional[Eiendeler] = None
    egenkapitalOgGjeld: Optional[EgenkapitalOgGjeld] = None


# --- Section: noteopplysninger ---

class NoteTabellRad(BaseModel):
    """
    Union-type row covering all note table permutations found in Norwegian årsregnskap.

    Column patterns by note type:
    - Two-year comparison (lønnskostnader, varelager, andre driftskostnader, revisjonshonorar):
        beskrivelse + currentYear + priorYear
    - Fixed asset movement (varige driftsmidler / immaterielle eiendeler):
        kategori + anskaffelseskost* + tilgang + avgang + akkumulerteAvskrivninger + bokfortVerdi + avskrivninger + levetid
    - Equity movement (egenkapitalendring):
        beskrivelse + aksjekapital + overkursfond + annenInnskuttEgenkapital + annenEgenkapital + sumEgenkapital
    - Shareholder info (aksjonærinformasjon):
        navn + antallAksjer + eierandel + aksjekapitalAndel + stemmeandelProsent
    - Executive/board remuneration (ytelser til ledende personer):
        rolle/navn + lonn + pensjon + annenGodtgjorelse + sumYtelser
    - Pledged assets (pantestillelser/sikkerhetsstillelser):
        type + bokfortVerdi + pantsattGjeld
    - Long-term debt maturity (langsiktig gjeld):
        type + totalGjeld + forfallInnen1Aar + forfallOver5Aar + restgjeld
    - Related-party loans (lån til nærstående):
        navn/rolle + lanebelop + rentesats + tilbakebetalingsvilkaar
    - Temporary differences (midlertidige forskjeller):
        beskrivelse + currentYear + priorYear + endring
    - Intercompany (konsernmellomværende):
        selskap + fordring + gjeld
    - Guarantee/contingent liabilities (garantier/betingede forpliktelser):
        type + belop + motpart + utlopsdato
    - Segment reporting (segmentinformasjon, large enterprises):
        segment + driftsinntekter + driftsresultat + eiendeler
    """

    # --- Universal / two-year comparison columns ---
    beskrivelse: Optional[str] = Field(None, description="Row label / description / line item name.")
    currentYear: Optional[float] = Field(None, description="Current year amount (NOK).")
    priorYear: Optional[float] = Field(None, description="Prior year amount (NOK).")
    endring: Optional[float] = Field(None, description="Change between years (NOK or %).")

    # --- Fixed asset movement schedule ---
    kategori: Optional[str] = Field(None, description="Asset category name.")
    anskaffelseskostPrAaretsStart: Optional[float] = Field(None, description="Acquisition cost at start of year.")
    tilgangKjopteDriftsmidler: Optional[float] = Field(None, description="Additions / purchased assets during year.")
    tilgangEgentilvirkede: Optional[float] = Field(None, description="Additions / self-constructed assets during year.")
    avgangIAaret: Optional[float] = Field(None, description="Disposals during the year.")
    anskaffelseskostPrAaretsSlutt: Optional[float] = Field(None, description="Acquisition cost at end of year.")
    akkumulerteAvskrivninger: Optional[float] = Field(None, description="Accumulated depreciation.")
    akkumulerteNedskrivninger: Optional[float] = Field(None, description="Accumulated impairment.")
    bokfortVerdi: Optional[float] = Field(None, description="Book value / carrying amount.")
    aaretsOrdinaereAvskrivninger: Optional[float] = Field(None, description="Depreciation expense for the year.")
    aaretsNedskrivninger: Optional[float] = Field(None, description="Impairment expense for the year.")
    okonomiskLevetid: Optional[str] = Field(None, description="Economic useful life, e.g. '3-10 år'.")
    avskrivningsmetode: Optional[str] = Field(None, description="Depreciation method, e.g. 'lineær'.")

    # --- Equity movement ---
    aksjekapital: Optional[float] = Field(None, description="Share capital column.")
    overkursfond: Optional[float] = Field(None, description="Share premium column.")
    annenInnskuttEgenkapital: Optional[float] = Field(None, description="Other paid-in equity column.")
    annenEgenkapital: Optional[float] = Field(None, description="Other equity / retained earnings column.")
    sumEgenkapital: Optional[float] = Field(None, description="Total equity column.")
    fondForVurderingsforskjeller: Optional[float] = Field(None, description="Revaluation reserve fund.")

    # --- Shareholder info ---
    navn: Optional[str] = Field(None, description="Name of person, entity, or counterparty.")
    antallAksjer: Optional[int] = Field(None, description="Number of shares held.")
    eierandel: Optional[float] = Field(None, description="Ownership share (%).")
    aksjekapitalAndel: Optional[float] = Field(None, description="Share of total share capital (NOK).")
    stemmeandelProsent: Optional[float] = Field(None, description="Voting share (%).")

    # --- Executive / board remuneration ---
    rolle: Optional[str] = Field(None, description="Role or title (e.g. 'daglig leder', 'styreleder').")
    lonn: Optional[float] = Field(None, description="Salary / wages (NOK).")
    pensjon: Optional[float] = Field(None, description="Pension contribution (NOK).")
    annenGodtgjorelse: Optional[float] = Field(None, description="Other remuneration (NOK).")
    sumYtelser: Optional[float] = Field(None, description="Total remuneration (NOK).")
    naturalytelser: Optional[float] = Field(None, description="Benefits in kind (NOK).")

    # --- Pledged assets / security ---
    type: Optional[str] = Field(None, description="Type of asset, debt, guarantee, or pledge.")
    pantsattGjeld: Optional[float] = Field(None, description="Debt secured by pledge (NOK).")

    # --- Long-term debt maturity ---
    totalGjeld: Optional[float] = Field(None, description="Total debt amount (NOK).")
    forfallInnen1Aar: Optional[float] = Field(None, description="Due within 1 year (NOK).")
    forfall1Til5Aar: Optional[float] = Field(None, description="Due 1-5 years (NOK).")
    forfallOver5Aar: Optional[float] = Field(None, description="Due after 5 years (NOK).")
    restgjeld: Optional[float] = Field(None, description="Remaining debt / outstanding balance (NOK).")

    # --- Related-party loans ---
    lanebelop: Optional[float] = Field(None, description="Loan principal amount (NOK).")
    rentesats: Optional[float] = Field(None, description="Interest rate (%).")
    tilbakebetalingsvilkaar: Optional[str] = Field(None, description="Repayment terms description.")

    # --- Tax: temporary differences ---
    skattesats: Optional[float] = Field(None, description="Tax rate (%).")

    # --- Intercompany ---
    selskap: Optional[str] = Field(None, description="Related company name.")
    fordring: Optional[float] = Field(None, description="Receivable amount (NOK).")
    gjeld: Optional[float] = Field(None, description="Liability amount (NOK).")

    # --- Guarantees / contingent liabilities ---
    belop: Optional[float] = Field(None, description="Amount (NOK) for guarantee or contingent liability.")
    motpart: Optional[str] = Field(None, description="Counterparty name.")
    utlopsdato: Optional[date] = Field(None, description="Expiry date of guarantee.")

    # --- Segment reporting (large enterprises) ---
    segment: Optional[str] = Field(None, description="Business segment or geographic area.")
    driftsinntekter: Optional[float] = Field(None, description="Segment operating revenue (NOK).")
    driftsresultat: Optional[float] = Field(None, description="Segment operating result (NOK).")
    eiendeler: Optional[float] = Field(None, description="Segment assets (NOK).")

    # --- Catch-all for unanticipated columns ---
    ekstraFelter: Optional[dict[str, Optional[str | int | float]]] = Field(None, description="Additional columns not captured by named fields.")


class NoteTabell(BaseModel):
    """A structured table within a note."""
    tabellTittel: Optional[str] = None
    kolonner: Optional[list[str]] = Field(None, description="Original column headers as they appear in the source PDF.")
    rader: Optional[list[NoteTabellRad]] = None


class Noteopplysning(BaseModel):
    """A single note disclosure from the financial statements."""
    noteNummer: Optional[int] = Field(None, description="Note reference number as shown in the financial statements.")
    tittel: Optional[str] = Field(None, description="Title or subject of the note.")
    innhold: Optional[str] = Field(None, description="Full text content of the note.")
    tabeller: Optional[list[NoteTabell]] = Field(None, description="Structured tabular data within the note.")


# --- Section: ansattinformasjon ---

class LonnskostnaderSpesifisert(BaseModel):
    lonninger: Optional[AarsPar] = None
    arbeidsgiveravgift: Optional[AarsPar] = None
    pensjonskostnader: Optional[AarsPar] = None
    andreYtelser: Optional[AarsPar] = None
    sumLonnskostnader: Optional[AarsPar] = None


class Ansattinformasjon(BaseModel):
    antallAarsverk: Optional[float] = Field(None, description="Number of FTE employees during the fiscal year.")
    lonnskostnaderSpesifisert: Optional[LonnskostnaderSpesifisert] = None


# --- Section: anleggsmidlerDetaljer ---

class AnleggsmiddelKategori(BaseModel):
    kategoriNavn: Optional[str] = None
    anskaffelseskostPrAaretsStart: Optional[int] = None
    tilgangKjopteDriftsmidler: Optional[int] = None
    avgangIAaret: Optional[int] = None
    anskaffelseskostPrAaretsSlutt: Optional[int] = None
    akkumulerteAvskrivninger: Optional[int] = None
    bokfortVerdi: Optional[int] = None
    aaretsOrdinaereAvskrivninger: Optional[int] = None
    okonomiskLevetid: Optional[str] = Field(None, description="Economic useful life, e.g. '3-10 år'.")


class AnleggsmidlerDetaljer(BaseModel):
    kategorier: Optional[list[AnleggsmiddelKategori]] = None


# --- Section: skattedetaljer ---

class SkattekostnadSpesifisert(BaseModel):
    betalbarSkatt: Optional[AarsPar] = None
    endringIUtsattSkatt: Optional[AarsPar] = None
    sumSkattekostnad: Optional[AarsPar] = None


class BeregningAvSkattegrunnlag(BaseModel):
    resultatForSkattekostnad: Optional[AarsPar] = None
    permanenteForskjeller: Optional[AarsPar] = None
    endringIMidlertidigeForskjeller: Optional[AarsPar] = None
    avgittKonsernbidrag: Optional[AarsPar] = None
    aaretsSkattegrunnlag: Optional[AarsPar] = None


class MidlertidigForskjell(BaseModel):
    beskrivelse: Optional[str] = None
    inneværendeAar: Optional[int] = None
    forrigeAar: Optional[int] = None
    endring: Optional[int] = None


class UtsattSkattefordelEllerSkatt(BaseModel):
    inneværendeAar: Optional[int] = None
    forrigeAar: Optional[int] = None
    skattesats: Optional[float] = Field(None, description="Tax rate as percentage, e.g. 22.")


class Skattedetaljer(BaseModel):
    skattekostnadSpesifisert: Optional[SkattekostnadSpesifisert] = None
    beregningAvSkattegrunnlag: Optional[BeregningAvSkattegrunnlag] = None
    midlertidigeForskjeller: Optional[list[MidlertidigForskjell]] = None
    utsattSkattefordelEllerSkatt: Optional[UtsattSkattefordelEllerSkatt] = None


# --- Section: egenkapitalendring ---

class EgenkapitalBevegelse(BaseModel):
    beskrivelse: Optional[str] = None
    aksjekapital: Optional[int] = None
    annenEgenkapital: Optional[int] = None
    sumEgenkapital: Optional[int] = None


class EgenkapitalSaldo(BaseModel):
    aksjekapital: Optional[int] = None
    annenEgenkapital: Optional[int] = None
    sumEgenkapital: Optional[int] = None


class Egenkapitalendring(BaseModel):
    bevegelser: Optional[list[EgenkapitalBevegelse]] = None
    inngaaendeBalanse: Optional[EgenkapitalSaldo] = None
    utgaaendeBalanse: Optional[EgenkapitalSaldo] = None


# --- Section: bankinnskuddDetaljer ---

class BankinnskuddDetaljer(BaseModel):
    bundneMidler: Optional[float] = Field(None, description="Restricted cash / bound funds (e.g. skattetrekkskonto).")
    sumBankinnskudd: Optional[float] = None


# --- Section: regnskapsprinsipper ---

class Regnskapsprinsipper(BaseModel):
    genereltGrunnlag: Optional[str] = Field(None, description="General basis of preparation.")
    inntektsforing: Optional[str] = Field(None, description="Revenue recognition policy.")
    varebeholdninger: Optional[str] = Field(None, description="Inventory valuation method.")
    varigeDriftsmidler: Optional[str] = Field(None, description="Fixed asset depreciation policy.")
    fordringer: Optional[str] = Field(None, description="Receivables valuation and impairment policy.")
    skatt: Optional[str] = Field(None, description="Tax accounting policy.")
    pensjon: Optional[str] = Field(None, description="Pension accounting policy.")


# --- Section: revisjonsberetning ---

class Revisjonsberetning(BaseModel):
    revisorNavn: Optional[str] = None
    revisjonsfirma: Optional[str] = None
    revisorTittel: Optional[str] = Field(None, description="e.g. 'statsautorisert revisor'.")
    beretningsdato: Optional[date] = None
    konklusjon: Optional[Revisjonskonklusjon] = None
    konklusjonSammendrag: Optional[str] = Field(None, description="Summary of the auditor's conclusion/opinion.")


# --- Section: signaturer ---

class Signatur(BaseModel):
    navn: Optional[str] = None
    rolle: Optional[str] = Field(None, description="Role or title, e.g. 'styreleder', 'daglig leder'.")
    signaturdato: Optional[date] = None


# --- Root model ---

class Aarsregnskap(BaseModel):
    """
    Complete extraction schema for a Norwegian årsregnskap (annual accounts) PDF.

    Covers: generell informasjon, resultatregnskap, balanse, noteopplysninger,
    ansattinformasjon, anleggsmidler detaljer, skattedetaljer, egenkapitalendring,
    bankinnskudd detaljer, regnskapsprinsipper, revisjonsberetning, and signaturer.
    """
    generellInformasjon: Optional[GenerellInformasjon] = None
    resultatregnskap: Optional[Resultatregnskap] = None
    balanse: Optional[Balanse] = None
    noteopplysninger: Optional[list[Noteopplysning]] = None
    ansattinformasjon: Optional[Ansattinformasjon] = None
    anleggsmidlerDetaljer: Optional[AnleggsmidlerDetaljer] = None
    skattedetaljer: Optional[Skattedetaljer] = None
    egenkapitalendring: Optional[Egenkapitalendring] = None
    bankinnskuddDetaljer: Optional[BankinnskuddDetaljer] = None
    regnskapsprinsipper: Optional[Regnskapsprinsipper] = None
    revisjonsberetning: Optional[Revisjonsberetning] = None
    signaturer: Optional[list[Signatur]] = None

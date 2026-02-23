# Extraction Reference: Norwegian Årsregnskap Notes

Compiled from structural audit, note compilations (FY2022–FY2024), glossary, terminology lists, bankinnskudd survey, verdipapirer compilation, table type taxonomy, union/labor disclosure audit, and note cross-reference index. Intended to inform field mapping, validation logic, and edge-case handling in the Pydantic extraction schema.

---

## 1. Note Structure Classes

Four primary structure classes observed across the sample set:

| Class | Label | Description |
|-------|-------|-------------|
| Type A | Narrative | Free-form text, may span multiple pages, bolded sub-headings for distinct policies |
| Type B | Key-Value | Simple label-value pairs, often single-line items |
| Type C | Standard Grid | Tabular with consistent columns (typically beskrivelse + currentYear + priorYear) |
| Type D | Complex Matrix | Multiple sub-tables under one note number, embedded calculations, subtotals, variable column counts |

Many notes are **mixed** (e.g., Type A/C for Gjeld, Type C with trailing narrative for Lønnskostnader).

### 1.1 Table Type Taxonomy (Systematic Classification)

Beyond note-level structure classes, the following table archetypes appear across the full årsregnskap document (including hovedregnskap and administrative sections):

| Category | Table Type | Column Pattern | Example Source |
|----------|-----------|----------------|----------------|
| Hovedregnskap | Resultatregnskap | Beskrivelse \| currentYear \| priorYear | All entities |
| Hovedregnskap | Balanse (Eiendeler) | Beskrivelse \| Beløp (or currentYear \| priorYear) | All entities |
| Hovedregnskap | Balanse (EK og Gjeld) | Beskrivelse \| Beløp (or currentYear \| priorYear) | All entities |
| Eiendeler-note | Varige driftsmidler (movement) | Beskrivelse \| [AssetClass1] \| [AssetClass2] \| ... \| Sum | Vitux AS, PEOPLE PERFORMANCE, Hofsfossveien 3 |
| Eiendeler-note | Verdipapirer / Investeringer | Selskap \| Eierandel/Type \| Anskaffelseskost \| Bokført verdi | Atski AS, SSA-SN Holding, Jacob K. AS |
| EK/Gjeld-note | Egenkapital (movement) | Beskrivelse \| Aksjekapital \| [Overkurs] \| Annen EK \| Sum EK | Atski AS, Mosaïque, Vertshuset Gibostad |
| EK/Gjeld-note | Gjeld (spesifikasjon) | Type gjeld \| currentYear \| priorYear | Syversen Kassefabrikk, Hofsfossveien 3, Jbg 13 |
| EK/Gjeld-note | Pantstillelser og garantier | Beskrivelse \| currentYear \| priorYear | Syversen Kassefabrikk, Blekkan Utvikling |
| Resultat-note | Skattekostnad (multi-sub-table) | Beskrivelse \| currentYear \| [priorYear] \| [Endring] | Atski AS, Asle Nilsen, Jacob K. AS, Mosaïque |
| Resultat-note | Lønnskostnader | Beskrivelse \| currentYear \| priorYear | PEOPLE PERFORMANCE, Vitux AS, Syversen Kassefabrikk |
| Selskapsinfo | Aksjekapitalens sammensetning | Aksjeklasse \| Antall \| Pålydende \| Bokført | Atski AS, Asle Nilsen, SR Gjøen, Vertshuset Gibostad |
| Selskapsinfo | Eierstruktur | Aksjonær \| Antall \| Eierandel% \| Stemmeandel% | Atski AS, Asle Nilsen, SR Gjøen |
| Selskapsinfo | Mellomværende nærstående | Beskrivelse \| Forhold \| currentYear \| priorYear | TONGANE 13, OBOS Vetslandsveien 68, GL-Bygg |
| Administrativ | Styrets sammensetning | Rolle \| Navn | Hans Nordahls Gate 68-70 SE, Syversen Kassefabrikk |
| Administrativ | Vedlegg/Saksdokumenter | Nr \| Beskrivelse \| Avsender \| Dato | Hans Nordahls Gate 68-70 SE |

**Key structural observation:** The resultatregnskap and balanse follow a strict hierarchical layout where driftsrelaterte poster appear first, then finansposter, then skatt — producing a trinnvis (step-wise) derivation of årsresultat. The balanse enforces the accounting equation (Eiendeler = Egenkapital + Gjeld) as a structural invariant. All downstream note tables provide detail for specific line items in these two hovedregnskap tables.

---

## 2. Note-by-Note Extraction Patterns

### 2.1 Regnskapsprinsipper (Accounting Principles)

**Structure:** Type A (Narrative). High frequency.

**Observed sub-headings** (bolded or caps, act as section delimiters):
- Driftsinntekter / Driftsinntekter og kostnader
- Salgsinntekter
- Inntekt og kostnad / Inntekt - avkastning
- Inntektsføring
- Klassifisering og vurdering av balanseposter
- Klassifisering og vurdering av anleggsmidler
- Klassifisering og vurdering av omløpsmidler
- Hovedregel for vurdering og klassifisering
- Varige driftsmidler
- Fordringer
- Varer / Varebeholdning
- Aksjer og andeler / Vurdering av aksjer
- Investering i andre selskaper / Tilknyttede selskaper
- Skatt / Inntektskatt
- Bruk av estimater
- Valuta
- Pensjonsforpliktelser (inline, e.g., "Forsikret pensjonsforpliktelse er ikke balanseført")
- Leieavtaler (inline, e.g., "Leieavtaler er ikke balanseført")
- Konsernforhold

**Extraction-relevant observations:**
- Some entities include a **principle change declaration** as a standalone sentence: "Selskapet har ikke endret regnskapsprinsipp fra [year] til [year]." Extract as boolean `prinsippEndret` + years.
- The standard framework declaration is nearly universal: "Årsregnskapet er satt opp i samsvar med regnskapsloven og [NRS 8 / god regnskapsskikk for små foretak]." Extract the specific standard referenced.
- Vitux AS uses **Forenklet IFRS** (simplified IFRS) — different framework, different expected note depth.
- Some entities (HAJ Holding AS) use equity method for subsidiaries — "Investeringer i tilknyttet selskap er vurdert etter egenkapitalmetoden." This is a key accounting choice to extract.
- Felloni Spekehus AS explicitly mentions "Bruk av estimater" — not all entities include this.

**Potential extraction error:** The Regnskapsprinsipper note sometimes appears **twice** in a single filing — once in the BRREG template (Part 1) and once in the uploaded attachment (Part 2). ROTO DRILL AS demonstrates this: two separate Regnskapsprinsipper entries with slightly different wording ("omløpsmiddel" vs "omløpsmidler"). The extraction pipeline must handle deduplication or flag dual instances.

### 2.2 Lønnskostnader (Payroll Costs)

**Structure:** Type C (Standard Grid), often **bifurcated** into cost table + supplemental narrative/data.

**Standard table columns:** beskrivelse | currentYear | priorYear

**Standard row labels observed:**
- Lønn / Lønninger
- Arbeidsgiveravgift / Folketrygdavgift
- Pensjonskostnader
- Andre ytelser
- Refusjoner (sometimes merged with Andre ytelser as "Andre ytelser / Refusjoner")
- Sum

**Supplemental data typically follows the table (not inside it):**
- Antall årsverk (man-years) — sometimes as standalone sentence, sometimes as separate line
- Antall ansatte (headcount) — distinct from årsverk; Vitux AS reports both (70.7 årsverk, 71 ansatte)
- OTP declaration — "Selskapet er pliktig til å ha tjenestepensjonsordning..." or "Selskapet er ikke pliktig..."
- Principle change note on pensions
- Ytelser til ledende personer — may be "Det er ikke utbetalt lønn til ledende ansatte" or explicit amounts
- Lån og sikkerhetsstillelse — "Selskapet har ikke gitt lån eller sikkerhetsstillelse..."
- Revisjonshonorar — sometimes embedded in same note, sometimes separate

**Extraction-relevant observations:**
- The zero-employee pattern is extremely common among holding companies: "Selskapet har ingen ansatte" / "[Company] har ikke hatt lønns- eller personalkostnader i [year]"
- Antall årsverk can appear as decimal (0.75, 1.0, 70.1) or integer (0, 1, 26)
- TH DRIPT uses unusual formatting: amounts appear as "1102.00" (with decimal) rather than standard Norwegian number formatting
- Vitux AS includes: "Lønnskostnader som er forbundet med investeringer utgjør kr. 5 757 256. Disse er aktivert i balansen." — capitalized labor costs, important for reconciliation
- Some entities report "Daglig leder og styret har ikke mottatt lønn" as a single combined statement
- Revisjonshonorar can be broken into sub-categories: Lovpålagt revisjon, Andre tjenester
- "Fravalg revisor" (opted out of audit) appears frequently for small entities

**Common combined note titles:**
- "Lønnskostnader og ytelser, godtgjørelser til daglig leder, styret og revisor"
- "Ansatte, godtgjørelser, lån til ansatte mv."
- "Ansatte og godtgjørelse til revisor"
- "Lønnskostnader, ingen ansatte"
- "Antall årsverk i regnskapsåret" (standalone, separate note)

### 2.3 Skatt (Tax)

**Structure:** Type D (Complex Matrix). High frequency. **Multiple distinct sub-tables under one note number.**

**Sub-table 1: Årets skattekostnad** (Tax expense for the year)
Standard rows:
- Resultatført skatt på ordinært resultat (header/label row)
- Betalbar skatt
- Endring i utsatt skatt / Endring i utsatt skattefordel
- Skattekostnad ordinært resultat
Columns: beskrivelse | currentYear | priorYear

**Sub-table 2: Skattepliktig inntekt** (Taxable income)
Standard rows:
- Resultat før skatt / Ordinært resultat før skattekostnad
- Permanente forskjeller
- Endring i midlertidige forskjeller
- Mottatt konsernbidrag (if applicable)
- Fremførbart underskudd / Anvendelse av fremførbart underskudd
- Skattepliktig inntekt / Årets skattegrunnlag
Columns: beskrivelse | currentYear | priorYear

**Sub-table 3: Betalbar skatt i balansen** (Tax payable on balance sheet)
Standard rows:
- Betalbar skatt på årets resultat
- Sum betalbar skatt i balansen
Columns: beskrivelse | currentYear | priorYear

**Sub-table 4: Midlertidige forskjeller** (Temporary differences / deferred tax)
Standard rows vary significantly:
- Varige driftsmidler / Anleggsmidler / Driftsmidler inkl. goodwill
- Aksjer og andre verdipapirer / Aksjer og andre finansielle instrumenter
- Avsetninger mv
- Skattemessig fremførbart underskudd / Akkumulert fremførbart underskudd
- Sum / Netto forskjeller
- "Inngår ikke i beregningen av utsatt skatt" (exclusion row)
- Utsatt skattefordel / Utsatt skatt (22%)
Columns: beskrivelse | currentYear | priorYear | Endring (three-column variant)

**Extraction-relevant observations:**
- The skattesats (tax rate) often appears as a standalone row: "Skattesats 22%"
- Small entities frequently include: "I henhold til God regnskapsskikk for små foretak balanseføres ikke utsatt skattefordel." This is a **standard closing sentence** that means deferred tax asset is NOT recognized.
- Jacob K. AS includes additional rows for securities: "Verdireduksjon finansielle instr. vurdert til virkelig verdi," "Regnskapsmessig tap realisasjon av aksjer" — these are entity-specific permanent differences
- Camilla Lyngstad Holding AS uses different date labels for deferred tax columns: "01.01." and "09.11." (the latter being the liquidation date) instead of standard year labels
- The sub-tables are sometimes separated by narrative text, sometimes by whitespace only
- Column headers for sub-table 4 can be: currentYear | priorYear OR currentYear | priorYear | Endring (3 columns)
- "Skattereduserende forskjeller som ikke kan utlignes" appears as a reconciling row

**Potential extraction error:** Sub-table headers within the tax note are often **not numbered** — they appear as bold text or underlined text (e.g., "Skattepliktig inntekt:") rather than as formal table titles. The parser must recognize these as sub-table delimiters.

### 2.4 Egenkapital (Equity Reconciliation)

**Structure:** Type C (Standard Grid). High frequency.

**Standard table format — rows are chronological movements:**
- Pr. 01.01.[year] / Egenkapital 01.01.[year] / Pr 1.1.
- Årets resultat
- Avsatt utbytte / Foreslått utbytte
- Tilleggsutbytte
- Avgitt konsernbidrag
- Mottatt konsernbidrag
- Pr. 31.12.[year] / Egenkapital 31.12.[year]

**Standard columns (variable):**
Minimum: Aksjekapital | Annen egenkapital | Sum egenkapital
Extended: Aksjekapital | Annen innskutt egenkapital | Overkurs/Overkursfond | Annen egenkapital | Sum egenkapital
With losses: Aksjekapital | Annen innsk. EK | Udekket tap | Sum
Borettslag: may use Borettsinnskudd instead of Aksjekapital

**Extraction-relevant observations:**
- "Udekket tap" (uncovered loss) replaces "Annen egenkapital" when equity is negative — these are structurally equivalent positions but semantically opposite
- Mosaique Headhunting: "Annen innskutt egenkapital" can be negative (-7,666)
- MANNERÅK includes a "Sluttoppgjør" (final settlement) section below the equity table for entities under liquidation
- Ulsetstemma includes "Avgitt konsernbidrag" as a negative movement — important for group structure analysis
- Some entities present only opening and closing balances without movement rows (Sameiet Sofiesgate 1: just "Annen egenkapital 1/1" → "Årets resultat" → "Annen egenkapital 31.12")

### 2.5 Aksjonærer / Aksjekapital (Shareholders / Share Capital)

**Structure:** Type D (Complex Matrix). High frequency. **Almost always contains 2+ sub-tables.**

**Sub-table 1: Share capital composition**
Columns: [Aksjeklasse] | Antall | Pålydende | Bokført
Typical single row: "Ordinære aksjer"
Multi-class variant (Nanna L. AS, Ola L. AS): A-aksjer (voting) + B-aksjer (non-voting)

**Sub-table 2: Ownership structure**
Header text variants: "Eierstruktur" / "De største aksjonærene i % pr. 31.12 var:" / "Aksjonærer i % pr. 31.12:"
Columns: [Navn] | Ordinære | Eierandel | Stemmeandel
OR: [Navn] | Antall | Eierandel% | Aksjeklasse

**Sub-table 3 (optional): Management/board shareholdings**
Header: "Aksjer eiet av medlemmer i styret og daglig leder:" / "Aksjer og opsjoner eiet av..."
Columns: Navn | Verv | Ordinære
Closing row: "Totalt antall aksjer: [N]"

**Extraction-relevant observations:**
- Ownership is overwhelmingly 100% single-shareholder in small enterprises
- The shareholder name may be a person (Asle Nilsen) or a company (Trj Holding AS, Eurovema Mobility AB)
- VERTSHUSET GIBOSTAD: Combines role information inline: "Tor Egil Sebulonsen, Daglig leder og Styrets leder (Daglig leder, Styreleder)"
- Eierandel and Stemmeandel are typically identical for single-class structures; diverge for multi-class (Nanna L. AS: 1 A-share = 100% votes, 499 B-shares = 0% votes)
- Pålydende format varies: "1,0" / "1 000,0" / "100,00" / "1 000,00" / "2,0"
- "Sum" row at bottom of share capital table may omit Pålydende column value

### 2.6 Anleggsmidler / Varige driftsmidler (Fixed Assets)

**Structure:** Type D (Complex Matrix). High frequency.

**Standard reconciliation matrix — rows represent financial movements:**
- Anskaffelseskost pr. 01.01.[year] / Anskaffelseskost 01.01.[year]
- Tilgang / Tilgang kjøpte driftsmidler / Tilgang i året / Årets tilgang
- Avgang / Avgang i året
- = Anskaffelseskost 31.12.[year] / = Anskaffelseskost pr. 31.12.[year]
- Akkumulerte avskrivninger 31.12.[year] / Akk. av- og nedskr. pr 1/1 / Samlede avskrivninger...
- Akkumulerte avskrivninger pr. 31.12.[year]
- = Bokført verdi 31.12.[year] / Balanseført verdi pr. 31.12.[year] / Bokført verdi per 31.05.2023
- Årets ordinære avskrivninger / Årets avskrivninger / Årets av-/nedskrivninger

**Key-value data below matrix:**
- Økonomisk levetid: "12 år" / "5-7 år" / "3-8 år" / "5 år"
- Avskrivningsplan: "Lineær 20 %" / "saldo 30%" / "saldo 20%"
- Avskrivningssatser: "5 år"

**Column variants (asset categories):**
- Single column: just the asset type (e.g., "Bil", "Driftsløsøre, inventar o.l.")
- Multi-column: Bygninger og tomter | Maskiner og anlegg | Driftsløsøre, inventar ol. | Sum
- Extended: Tomt | Bygninger | El-bil ladeanlegg (Hofsfossveien 3)

**Extraction-relevant observations:**
- Row labels with "=" prefix (e.g., "= Anskaffelseskost 31.12.23") are **calculated/subtotal rows** — these are presentation artifacts, not independent data
- Eurovema Mobility uses non-standard date range: "01.06.2022" to "31.05.2023" — fiscal year ≠ calendar year
- Hofsfossveien 3: "Tomt og bygninger avskrives ikke" — zero depreciation for land/buildings in borettslag
- Atski AS 2022: Full disposal (avgang 5,232,525 = opening anskaffelseskost) → bokført verdi 0
- Empty cells in multi-column matrices are common (e.g., Bygninger column has values, Driftsløsøre column is empty for certain rows)
- Vitux AS includes: "Samlede aktiverte utgifter til forskning og utvikling i 2022 er 5 601 551" — R&D capitalization narrative below the matrix

### 2.7 Gjeld og Pantstillelser (Debt and Pledges)

**Structure:** Type A/C (Mixed). High frequency. Highly variable format.

**Common sub-sections within this note:**

**Pantsikret gjeld (Pledged debt):**
Columns: beskrivelse | currentYear | priorYear
Rows: Gjeld til kredittinstitusjoner, Sum pantsikret gjeld

**Balanseført verdi av pantsatte eiendeler (Book value of pledged assets):**
Columns: beskrivelse | currentYear | priorYear
Rows: Tomt og bygning / various asset types, Sum pantstillelser

**Debt maturity:**
- "Selskapet har ingen gjeld med forfall senere enn 5 år." (narrative)
- "Av langsiktig gjeld på kr 0,- forfaller kr 0 om mer enn 5 år." (narrative)
- "Del av gjeld som forfaller til betaling mer enn fem år etter regnskapsårets slutt: 560 005" (key-value)

**Loan details (Sameiet Sofiesgate 1 pattern):**
Individual loan entries with: Lender, Original amount, Interest rate, Repayment terms, Opening balance, Payments in year, Closing balance

**Extraction-relevant observations:**
- DANINOR AS: "Gjeld til aksjonær kr. 267.202,39" — pure narrative, no table. Note the Norwegian number format with period as thousands separator and comma as decimal.
- PEOPLE PERFORMANCE AS: Three separate debt disclosures as key-value pairs (maturity > 5 years, pledged debt amount, pledged asset values)
- Sameiet Sofiesgate 1: Detailed loan-by-loan breakdown with specific bank names and account references
- "Ubenyttet limit kassekreditt" (unused credit facility) may appear as supplemental key-value

### 2.8 Fordringer (Receivables)

**Structure:** Type A/C (Mixed). High frequency. Typically simple.

**Variants:**
1. Simple narrative: "Fordringer som forfaller senere enn ett år: [amount]"
2. Table: type | currentYear | priorYear
3. Key-value with party specification (DANINOR): individual fordringer to named group companies

**Extraction-relevant observations:**
- DANINOR AS: "Fordringer på personlige eiere, styremedlemmer mv." — this is a **related-party receivable** that may have a negative value (-1,022,566), indicating a payable rather than receivable
- Vitux AS Note 8: "Atradius AS stiller garanti for deler av kundefordringene" — credit insurance disclosure
- SYVERSEN KASSEFABRIKK: "Fordringer som forfaller senere enn ett år etter regnskapsårets slutt: 46 089" — simple key-value

### 2.9 Verdipapirer / Investeringer (Securities and Investments)

**Structure:** Type C/D. Medium-high frequency (investment vehicles, holding companies, entities with financial portfolios).

**Classification of investment types observed:**

| Investment Category | Balance Sheet Location | Valuation Method | Example Entities |
|--------------------|-----------------------|-----------------|-----------------|
| Aksjer i datterselskap | Finansielle anleggsmidler | Kostmetoden or egenkapitalmetoden | AEGIR AS (Orcas AS), Roto Drill AS (RD Boring og Sprengning AS) |
| Aksjer i tilknyttet selskap | Finansielle anleggsmidler | Kostmetoden or egenkapitalmetoden | Nanna L. AS (Truls AS 25%), Camilla Lyngstad (Willaks AS 50%), HAJ Holding (Kvernmo AS 12%, BB10 Utvikling AS 6.5%) |
| Anleggsmidler — other long-term | Finansielle anleggsmidler | Kostmetoden | Hagens Invest AS (Stensrudvegen 16 AS 50%, TPN Eiendom AS 50%), St. Olavs Gate 6 AS |
| Markedsbaserte aksjer/fond (omløpsmidler) | Investeringer (omløpsmidler) | Virkelig verdi or laveste verdis prinsipp | Atski AS, SR Gjøen AS, Vesterøyveien Invest AS, Trøseid AS |
| Obligasjons-/pengemarkedsfond | Investeringer (omløpsmidler) | Virkelig verdi | Jacob K. AS, Vesterøyveien Invest AS |
| Private Equity | Finansielle anleggsmidler or omløpsmidler | Kostmetoden or virkelig verdi | Dumas Holding AS (sum 2015–2022) |

**Atski AS pattern — detailed portfolio listing:**
Columns: [Selskap/Fond] | Anskaffelseskost | Markedsverdi
17 individual equity holdings listed by name (Aker ASA, DNB ASA, Orkla ASA, etc.)
Sum row at bottom. FY2022: AK 7 708 043 / MV 10 014 876. FY2023: AK 7 278 115 / MV 10 212 689.

**Dumas Holding AS pattern — diversified fund portfolio:**
12+ individual fund positions (AKO Global UCITS, Global Bonds, Nordic Equities, Nordic High Yield, Private Equity etc.) with individual anskaffelseskost and markedsverdi. Sum across all omløpsmidler: AK ~24.6M / MV ~27.0M (FY2022). Plus separate anleggsmiddel investment (Bright Group Oy: AK 3.9M).

**SR Gjøen AS pattern — aggregate:**
"Markedsbaserte aksjer/fond og andre finansielle instrumenter"
Columns: Virkelig verdi | Periodens resultatført verdijustering.
FY2022: 11.8M, FY2023: 19.3M.

**HAJ Holding AS pattern — egenkapitalmetoden:**
Investments in Kvernmo AS (12%) and BB10 Utvikling AS (6.5%) reported at equity method value (EK pr 01.01 / EK pr 31.12). No anskaffelseskost disclosed separately.

**Advokat Eirik Glad Balchen pattern — investment in associate:**
Columns: Kontorkommune | Eierandel | Kostpris | Bokført Verdi

**Extraction-relevant observations:**
- Individual security names are **proper nouns** (company names) — the parser must handle these as category labels, not as financial terms
- "Nedregulering verdipapirer" / "Netto gevinst realisasjon verdipapirer" appear in resultatregnskapet, not in the note itself
- SR Gjøen uses "kostmetoden" for some investments and "virkelig verdi" for trading portfolio — different valuation methods produce different note structures
- Egenkapitalmetoden entities (HAJ Holding) report opening and closing equity values rather than cost — requires different extraction fields (EK pr 01.01, Resultatandel, EK pr 31.12)
- Camilla Lyngstad Holding: Willaks AS investment goes from BV 15 000 (FY2022) to BV 0 (FY2023) — full writedown during liquidation
- AEGIR AS: Orcas AS fully nedskrevet (AK 30 000, BV 0) — zero book value but ownership retained
- Roto Drill AS: "Konstatert eierskap, ingen bokført verdi oppgitt i noter" — ownership disclosed without financial values
- Hagens Invest AS: mixed portfolio — anleggsmidler (Stensrudvegen 16 50%, TPN Eiendom 50%) + omløpsmidler (Kongsberg Gruppen ASA AK/BV 15 797)
- Vesterøyveien Invest: separate line items for markedsbaserte aksjer and markedsbaserte obligasjoner
- Lån til tilknyttet selskap (St. Olavs Gate 6 AS: 124 820) appears as a separate anleggsmiddel line, not inside the verdipapirer note — extraction must distinguish between equity investment and intercompany loan

### 2.10 Varelager (Inventory)

**Structure:** Type C (Simple Grid). Medium frequency.

**Standard rows:**
- Lager av innkjøpte varer / Innkjøpte varer for videresalg
- Råvarer / Lager av råvarer
- Mellomvarer
- Sum

**Extraction-relevant observations:**
- Vitux AS includes narrative: "En del av varene i varelageret er ikke lenger vurdert til anskaffelseskost, men nedskrevet til virkelig verdi" — impairment flag
- VERTSHUSET GIBOSTAD: very simple — single row "Lager av Innkjøpte varer"
- FIFO principle reference often appears in Regnskapsprinsipper, not in the inventory note itself

### 2.11 Bankinnskudd (Bank Deposits)

**Structure:** Type B (Key-Value). Medium-high frequency.

**Standard disclosure:**
"Av selskapets bankinnskudd pr 31.12.[year] er kr [amount] bundne skattetrekksmidler."

**Extended variant (Sameiet Sofiesgate 1):**
Individual account listing with bank name and account number: "Driftskonto DnB 1503.71.92349: 4 435 929"

**Observed disclosure patterns across FY2022–FY2024 sample:**

| Pattern | Example Entities | Note Behaviour |
|---------|-----------------|----------------|
| Bundne skattetrekksmidler (amount) | Syversen Kassefabrikk (Note 8/9), Vitux AS (Note 7), TVERRLIA VA-DRIFT (Note 6), Advokat Eirik Glad Balchen (Note 5), STORE NORSKE GRUVE 3 (Note 6), Hårek Frisørstudio (Note 4) | Amount stated as key-value sentence |
| Explicit zero bundne midler | HAJ Holding AS (Note 7): "kr 0,- bundne skattetrekksmidler. Skyldig skattetrekk pr 31/12 er kr 0,-" | Zero-value confirmation with cross-reference to skyldig skattetrekk |
| No skattetrekkskonto | Sameiet Sofies gate 1 (Note 3): "Skattetrekkskonto (har ikke): 0" | Explicit declaration of non-existence |
| No dedicated note | Jacob K. AS, Solsiden Boligsameie 1, KRAHN INVEST AS, M.S.K. HOLDING AS, Safe4 Care AS | Balance line item only, no supplemental note |
| Pantsatt bankinnskudd | Blekkan Utvikling AS (Note 5): bankinnskudd listed under "Oversikt pantsatte eiendeler" | Bank deposits as pledged collateral, cross-referenced from pantstillelser note |
| Driftskonto spesification | Sameiet Sofies gate 1 (Note 3): bank name + account number | Individual account-level breakdown |

**Extraction-relevant observations:**
- The key extraction target is "bundne skattetrekksmidler" (restricted tax withholding deposits) — mandatory disclosure
- Account numbers appear in some reports (Sameiet Sofies gate 1: "1503.71.92349") — PII consideration for storage
- "Skattetrekkskonto (har ikke): 0" — explicit declaration of no restricted deposits
- Pantsatt bankinnskudd (Blekkan Utvikling AS) is a distinct disclosure from bundne skattetrekksmidler — both restrict liquidity but for different reasons
- Hårek Frisørstudio adds forward-looking adequacy statement: "det er nok til å dekke termin som forfaller i januar 2025"
- HAJ Holding AS cross-references skyldig skattetrekk against bundne midler — extraction pipeline should capture both for validation
- Many entities (Jacob K., Solsiden, KRAHN, M.S.K., Safe4 Care) report bankinnskudd only as a balance line item with no note — absence of note is not an extraction error
- Note numbers for this disclosure: Note 3, 4, 5, 6, 7, 8, 9 observed — no standardization

### 2.12 Konsernforhold (Group Relations)

**Structure:** Type A/C (Mixed). Medium frequency.

**ROTO DRILL AS pattern:**
- "Investering som regnskapsføres etter egenkapitalmetoden" (heading)
- "Konsernregnskap" — "Virksomheten inngår i konsolideringen til morselskapets konsernregnsk.: Nei"

**Vitux AS pattern (detailed intercompany):**
Three sub-tables:
1. Fordringer på konsernselskap: Company | currentYear | priorYear
2. Langsiktig gjeld til konsernselskap: Company | currentYear | priorYear
3. Kortsiktig gjeld til konsernselskap: Company | currentYear | priorYear

**Extraction-relevant observations:**
- Vitux Canada Inc. and Vitux USA LLC appear as foreign subsidiaries with intercompany balances — currency risk implication
- Negative intercompany balances appear: "Vitux USA LLC: -1,399,348" — indicates a receivable classified under payables
- "Konsernkontoordning" (group account arrangement) may appear as a narrative disclosure

### 2.13 Fortsatt drift (Going Concern)

**Structure:** Type A (Narrative). Low-medium frequency (most entities omit or include one-liner).

**Standard confirmation:** "Er det usikkerhet om fortsatt drift? Nei"

**Detailed justification pattern (DANINOR AS):**
Multi-paragraph narrative explaining:
- Board's assessment basis
- Operational measures taken
- Budget assumptions
- Conclusion on going concern

**Extraction-relevant observations:**
- A detailed going concern note is a **red flag** — it signals the board felt compelled to address underlying financial pressures
- SYVERSEN KASSEFABRIKK: references "positive equity and booked receivables" as basis for going concern — conditional language
- Camilla Lyngstad Holding: entity under liquidation ("Selskapet er meldt oppløst") — going concern assumption does NOT apply

### 2.14 Fisjon/Fusjon (Demerger/Merger)

**Structure:** Type A (Narrative). Low frequency.

**Ammerudveien 19-25 AS pattern:**
- "Ammerudveien 19-25 AS har vært overdragende selskap i fisjon gjennomført i 2022"
- Names of overtakende selskaper (receiving entities)
- Method: "kontinuitet etter reglene for skattefri fisjon"

### 2.15 Borettslag/Sameie-Specific Notes

**Structure:** Mixed. Appear only for housing cooperatives.

**Unique note types not found in commercial entities:**
- Styrets arbeid (Board's work) — narrative about meetings, maintenance, disputes
- HMS (Health, Safety, Environment)
- Kontrakter og avtaler (Contracts and agreements)
- Økonomi (Economy) — narrative about budget performance, cost increases
- Planer for [next year]
- Kommentarer til budsjett (Budget commentary)
- Vedlikehold (Maintenance) — itemized repair costs with vendor names
- Kommunale avgifter (Municipal charges) — vann/avløp, renovasjon
- Andre driftsinntekter — with specific account numbers (3606, 3610)

**Extraction-relevant observations:**
- These entities use **account numbers** (kontonummer) as row identifiers: "5300 Styrehonorar", "6700 Revisjon"
- Hofsfossveien 3 includes **antatt levetid** as a year (2016) rather than a duration — this appears to be the acquisition/construction year, not economic life. **Likely data quality issue.**
- Hans Nordahls Gate 68-70 SE includes non-financial documents: application to Plan- og bygningsetaten, rejection letter, appeal — these are **noise** that must be filtered
- Felleskostnader (common charges) replace driftsinntekter as the primary revenue concept
- "Inndekning av kapitalkostnader (IN)" appears as a negative revenue item

### 2.16 Salgsinntekt (Revenue)

**Structure:** Type A/B (Narrative or Key-Value). Low-medium frequency as standalone note.

**Observed patterns:**
- Overview of selskapets omsetning with description of virksomhetens art
- Revenue breakdown by activity type (e.g., "nybygg og rehabilitering")
- Sometimes merged with Regnskapsprinsipper under "Driftsinntekter" sub-heading
- Segment-like breakdowns in some entities (geographical or product/service categories)

**Referanse til regnskapspost:** Resultatregnskap: Salgsinntekt / Driftsinntekter

**Extraction-relevant observations:**
- Standalone salgsinntekt notes are uncommon for small enterprises — revenue detail more often appears as a sub-heading within Regnskapsprinsipper or as a single resultatregnskap line
- When present as a separate note, often contains virksomhetens art description (nature of business) which is valuable for industry classification
- Observed note number: Note 1

### 2.17 Annen kortsiktig gjeld (Other Current Liabilities)

**Structure:** Type C (Standard Grid). Low-medium frequency as standalone note.

**Standard table columns:** beskrivelse | currentYear | priorYear

**Common row labels:**
- Forskudd fra kunder
- Skyldige feriepenger
- Skyldig arbeidsgiveravgift
- Avsatte kostnader / Påløpte kostnader
- Skyldig mva
- Sum annen kortsiktig gjeld

**Referanse til regnskapspost:** Balanse: Kortsiktig gjeld / Annen kortsiktig gjeld

**Extraction-relevant observations:**
- This note provides the composition breakdown for the aggregate "Annen kortsiktig gjeld" balance line
- Forskudd fra kunder (advances from customers) is a credit risk indicator — large advances signal either project-based revenue or potential revenue reversal risk
- Skyldige feriepenger is a statutory obligation directly derivable from lønnskostnader — provides cross-validation against the lønnskostnader note
- Observed note number: Note 10

### 2.18 Fagforeninger og Tariffavtaler (Union Relations and Collective Agreements)

**Structure:** Type A (Narrative). Low frequency in NRS 8 filings. Higher frequency in large entity årsberetning.

**Regulatory basis:**
- Large enterprises and ASA: Mandatory disclosure in Årsberetning (Section 5: Working Environment / Arbeidsmiljø) under Regnskapsloven Chapter 7
- Small enterprises (NRS 8): Generally exempt from preparing Årsberetning — union/collective agreement disclosures structurally absent
- Entities with "limited accounting obligation" (begrenset regnskapsplikt): No disclosure expected

**Key search terms for extraction:**
- Samarbeid med tillitsvalgte (Cooperation with shop stewards)
- Fagforeninger (Labor unions)
- Tariffavtale (Collective bargaining agreement)
- Hovedavtalen (The Basic Agreement)
- Arbeidsmiljø (Working environment)

**Extraction-relevant observations:**
- Zero-employee entities (OBOS Vetlandsveien 68 AS, Manneråk Fler Service AS: 0.00 årsverk) have no workforce — union disclosure is functionally non-existent
- **False positive risk:** Sameiet Sofies gate 1 uses "tillitsvalgte" to refer to the Styre (Board of Directors) under Eierseksjonsloven (Condominium Act), NOT labor union stewards. Keyword extraction without contextual validation produces false positives.
- **Chain of responsibility pattern:** OBOS Vetlandsveien 68 AS Note 3 reveals management is employed by OBOS Eiendom AS and leased to the company — substantive labor disclosure burden exists at the parent level, not the subsidiary
- The A-melding reporting system provides the primary audit trail for workforce scale verification — reconciliation of salary costs and holiday pay basis against the resultatregnskap
- Upcoming IFRS Sustainability Disclosure Standards (IFRS S2 / N_ESG) will formalize union cooperation and social metrics as verified statutory disclosures for large entities from FY2024 onward
- For the extraction pipeline: treat union/collective agreement data as an **årsberetning-only** extraction target, not a note-level target, for NRS 8 entities

---

## 3. Number Format Patterns

Norwegian number formatting observed across the sample:

| Pattern | Example | Context |
|---------|---------|---------|
| Space as thousands separator, no decimal | 1 230 450 | Most common for NOK amounts |
| Period as thousands separator, comma decimal | 267.202,39 | Older/narrative style (DANINOR) |
| Comma decimal, no thousands sep | 1 000,0 | Pålydende (par value) |
| Negative in parentheses | (161 356) | VERTSHUSET GIBOSTAD fixed assets |
| Negative with minus prefix | -7 666 | Egenkapital (Mosaique) |
| Negative with minus prefix, no space | -1 022 566 | Fordringer (DANINOR) |
| Decimal with period | 1102.00 | TH DRIPT — legacy system format |
| Percentage | 100,0 / 100,00% / 11,1% | Eierandel |
| Zero as dash | - | Common for nil values (Camilla Lyngstad) |
| Zero as 0 | 0 | Also common |
| Zero as blank/empty cell | | Common in multi-column matrices |

**Critical:** The extraction pipeline must handle all variants. "267.202,39" and "267 202" represent fundamentally different amounts (267k vs 267k — same in this case, but the formatting logic differs). The period-as-thousands pattern is a trap for parsers expecting period-as-decimal.

---

## 4. Note Title/Number Conventions

No standardized note numbering exists. The same disclosure type gets different numbers across entities:

| Disclosure | Observed Note Numbers |
|------------|----------------------|
| Regnskapsprinsipper | Unnumbered, Note 1, "Note Regnskapsprinsipper" |
| Lønnskostnader | Note 1, Note 2, Note 4 |
| Skatt | Note 1, Note 3, Note 4, Note 5, Note 6, Note 7, Note 12, Note 13 |
| Egenkapital | Note 2, Note 3, Note 4, Note 5, Note 6, Note 9, Note 10 |
| Aksjonærer | Note 2, Note 3, Note 4, Note 5, Note 8, Note 9 |
| Anleggsmidler | Note 1, Note 2, Note 3, Note 5, Note 6 |
| Pantstillelser | Note 5, Note 6, Note 7, Note 11 |
| Fortsatt drift | Note 4, unnumbered |
| Verdipapirer | Note 4, Note 7 |
| Fordringer | Note 2, Note 4, Note 5 |
| Bankinnskudd/Bundne midler | Note 3, Note 4, Note 5, Note 6, Note 7, Note 8, Note 9 |
| Nærstående/Konsern | Note 4, Note 5 |
| Salgsinntekt | Note 1 |
| Annen kortsiktig gjeld | Note 10 |

**Implication:** Note numbers are **not** semantic identifiers. Extraction must be **content-based** (semantic), not position-based.

**Title variants for the same disclosure:**
- "Lønnskostnader og ytelser, godtgjørelser til daglig leder, styret og revisor"
- "Ansatte, godtgjørelser, lån til ansatte mv."
- "Ansatte og godtgjørelse til revisor"
- "Lønnskostnader, ingen ansatte"
- "Lønnskostnader etc"
- "Antall årsverk i regnskapsåret" (standalone)

### 4.1 Note Cross-Reference Index (Note → Regnskapspost Mapping)

The following index maps observed note disclosures to their primary regnskapspost references, sourced across the FY2022–FY2024 sample set. The "Kilde" column refers to source entity groups.

| Observed Note Numbers | Note Title | Content Description | Referanse til regnskapspost | FY | Kilde |
|----------------------|------------|--------------------|-----------------------------|-----|-------|
| Note 1, 5, 6 | Skatt / Skattekostnad på ordinært resultat | Spesifikasjon av årets skattegrunnlag, betalbar skatt, midlertidige forskjeller og utsatt skattefordel. | Resultatregnskap: Skattekostnad / Balanse: Utsatt skattefordel/skatt | 2022 | [1], [2], [3] |
| Note 2, 3, 8 | Selskapskapital / Aksjonærer / Aksjekapital | Informasjon om selskapets aksjonærer, eierstruktur, antall aksjer, pålydende og eierandel. | Balanse: Selskapskapital / Aksjekapital | 2022 | [1], [2], [3] |
| Note 2, 3, 9 | Egenkapital / Selskapskapital | Oversikt over endringer i egenkapital, aksjekapital, annen innskutt egenkapital og annen egenkapital. | Balanse: Egenkapital / Sum egenkapital | 2022 | [1], [2], [3] |
| Note 4, 5, 4 | Nærstående / Mellomværende med selskap i samme konsern / Transaksjoner med nærstående parter | Oversikt over transaksjoner, kjøp, salg og gjeld til nærstående parter og konsernselskap. | Balanse: Kortsiktig gjeld til konsernselskap / Leverandørgjeld / Resultatregnskap: Driftskostnader/inntekter | 2022 | [1], [2], [3] |
| Note 1, 3 | Anleggsnote / Avskrivning på varige driftsmidler | Oversikt over anskaffelseskost, akkumulerte avskrivninger og bevegelsestabell for driftsmidler. | Balanse: Maskiner og anlegg / Varige driftsmidler / Resultatregnskap: Avskrivninger | 2022 | [2], [3] |
| Note 4, 2 | Lønnskostnader og ytelser / Lønnskostnader | Detaljer om lønn, arbeidsgiveravgift, pensjonskostnader, godtgjørelser til ledelse/revisor og antall årsverk. | Resultatregnskap: Lønnskostnad | 2022 | [2], [3] |
| Note 1 | Salgsinntekt | Oversikt over selskapets omsetning og virksomhetens art. | Resultatregnskap: Salgsinntekt | 2022 | [3] |
| Note 6 | Pantstillelser, Garantiansvar | Informasjon om stilte garantier, kassekreditt og pantstilte eiendeler. | Balanse: Varige driftsmidler / Fordringer (som pant) | 2022 | [3] |
| Note 7 | Bundne midler | Opplysning om bundne skattetrekksmidler på bankkonto. | Balanse: Bankinnskudd | 2022 | [3] |
| Note 10 | Annen kortsiktig gjeld | Spesifikasjon av kortsiktige forpliktelser som forskudd fra kunder og skyldige feriepenger. | Balanse: Kortsiktig gjeld | 2022 | [3] |

**Usage for extraction pipeline:** This index enables bidirectional lookup: (1) given a note number from a specific entity, determine which regnskapspost it likely maps to, and (2) given a regnskapspost line item, determine which note numbers to search for across the document.

---

## 5. Critical Parsing Anomalies

### 5.1 Legacy System Text Dumps
- Fixed-width character-position layouts (TH DRIPT pattern)
- Horizontal rules as repeating underscore/hyphen characters
- Amount formatting with decimals ("1102.00") atypical for Norwegian standards
- Requires fixed-width column detection rather than delimiter-based parsing

### 5.2 Dual Occurrence of Same Note
- ROTO DRILL AS: Regnskapsprinsipper appears in both Part 1 (template) and Part 2 (attachment) with minor textual differences
- The pipeline must either deduplicate or flag for manual review

### 5.3 Non-Financial Content in PDF
- Hans Nordahls Gate 68-70 SE: includes Årsmøte minutes, municipal correspondence, appeals
- Must classify and filter pages before note extraction

### 5.4 Signatures and Digital Artifacts
- Scanned handwritten signatures disrupt text flow
- BankID signing stamps embedded as image objects
- "Sealed by Verified" full-page verification logs
- Fødselsnummer (11-digit national ID) may appear in signature blocks — **PII redaction required**

### 5.5 Empty/Boilerplate Reports
- Some PDFs are blank templates with standard headings but no financial data
- Incomplete: filled headings alongside unfilled sections (FSL HOLDING AS)
- The pipeline must detect and flag these rather than treating empty fields as zero

### 5.6 Entities Under Liquidation
- Camilla Lyngstad Holding: "Selskapet er meldt oppløst, og vil bli slettet i løpet."
- MANNERÅK FLER. SERVICE: avviklingsregnskap with "Sluttoppgjør" section
- These entities have non-standard reporting dates and equity structures

### 5.7 Non-Calendar Fiscal Years
- Eurovema Mobility AS: fiscal year 01.06.2022 to 31.05.2023
- The Regnskapsår field on the cover page is the authoritative source

### 5.8 Keyword False Positives in Non-Commercial Entities
- Sameiet Sofies gate 1: "tillitsvalgte" refers to the Styre (Board of Directors) under Eierseksjonsloven, NOT labor union stewards
- Automated keyword extraction for "tillitsvalgte" / "fagforening" / "tariffavtale" must include contextual validation to distinguish sameie/borettslag governance terminology from labor relations terminology
- Similar risk with "årsmøte" (general meeting for sameie) vs "generalforsamling" (general meeting for AS)

### 5.9 Chain-of-Responsibility for Personnel Disclosures
- OBOS Vetlandsveien 68 AS: management employed by OBOS Eiendom AS and leased to the entity — personnel cost and headcount disclosures exist at parent level, not subsidiary
- The subsidiary report shows "0 årsverk" despite having active management — not a data quality issue but a structural consequence of the employment arrangement
- Extraction pipeline must flag entities where "Daglig leder er ansatt i [other entity]" appears, indicating disclosure burden is elsewhere

---

## 6. Comprehensive Terminology Reference

### 6.1 Resultatregnskap Line Items

**Inntekter:** Driftsinntekter, Salgsinntekt, Sum driftsinntekter

**Kostnader:** Varekostnad/Vareforbruk, Lønnskostnad, Annen driftskostnad/Andre driftskostnader, Sum driftskostnader

**Driftsresultat** (Operating result)

**Finansposter:** Finansinntekter, Renteinntekter, Annen finansinntekt, Finanskostnader, Annen finanskostnad, Annen rentekostnad, Nedregulering verdipapirer, Netto gevinst realisasjon verdipapirer, Netto tap ved salg av verdipapirer, Netto finans/Netto finansinntekt, Resultat av finansposter

**Resultat:** Ordinært resultat før skattekostnad, Resultat før skattekostnad, Skattekostnad på ordinært resultat, Ordinært resultat etter skattekostnad, Årsresultat, Årsoverskudd/(Årsunderskudd), Totalresultat

**Disponeringer:** Overføringer, Overført til udekket tap, Sum overføringer

### 6.2 Balanse Line Items

**Anleggsmidler:** Immaterielle eiendeler (Goodwill, Konsesjoner/patenter, Utvikling), Varige driftsmidler (Bygg under oppføring, Driftsløsøre/inventar, Maskiner og anlegg, Tomter/bygninger), Finansielle anleggsmidler (Andre fordringer, Investeringer i tilknyttet selskap, Lån til tilknyttet selskap, Utsatt skattefordel)

**Omløpsmidler:** Varelager (Varer), Fordringer (Andre kortsiktige fordringer, Kundefordringer), Investeringer (Beholdning av egne aksjer, Markedsbaserte aksjer), Bankinnskudd/kontanter

**Egenkapital:** Innskutt (Aksjekapital, Annen innskutt egenkapital, Borettsinnskudd, Overkurs), Opptjent (Annen egenkapital, Udekket tap)

**Gjeld:** Langsiktig (Annen langsiktig gjeld, Gjeld til kredittinstitusjoner, Øvrig langsiktig gjeld), Kortsiktig (Annen kortsiktig gjeld, Betalbar skatt, Gjeld til aksjonær, Leverandørgjeld)

**Poster utenom balansen:** Pantstillelser

### 6.3 Note-Specific Terminology

**Skatt:** Betalbar skatt, Utsatt skatt, Utsatt skattefordel, Permanente forskjeller, Midlertidige forskjeller, Fremførbart underskudd, Skattepliktig inntekt, Effektiv skattesats

**Egenkapital movements:** Avsatt utbytte, Foreslått utbytte, Tilleggsutbytte, Avgitt konsernbidrag, Mottatt konsernbidrag

**Fixed assets:** Anskaffelseskost, Tilgang, Avgang, Akkumulerte avskrivninger, Ordinære avskrivninger, Bokført verdi/Balanseført verdi, Økonomisk levetid, Avskrivningsplan

**Debt/pledges:** Pantstillelse, Pantsikret gjeld, Gjeld sikret ved pant, Balanseført verdi av pantsatte eiendeler, Ubenyttet limit kassekreditt, Garantiforpliktelser

**Personnel:** Antall årsverk, Antall ansatte, OTP (Obligatorisk tjenestepensjon), Pensjonsforpliktelser, Ytelser til ledende personer, Styrehonorar, Revisjonshonorar, Lovpålagt revisjon

**Group:** Mellomværende med selskap i samme konsern, Konsernbidrag, Konsernkontoordning, Fordring konsernselskap, Gjeld til konsernselskap

**Going concern:** Fortsatt drift

### 6.4 Årsberetning Terminology

**Strategi/drift:** Virksomhetens art, Markedsutvikling, Resultat og finansiell stilling, Fremtidig utvikling/Fremtidsutsikter, Styrets arbeid

**Risiko:** Finansiell risiko, Markedsrisiko, Kredittrisiko, Likviditetsrisiko, Valutarisiko, Renterisiko, Operasjonell risiko, Klimarisiko

**Samfunnsansvar:** Arbeidsmiljø, HMS, Likestilling, Miljørapportering, Åpenhetsloven

### 6.5 Revisjonsberetning Terminology

Uavhengig revisors beretning, Konklusjon, Grunnlag for konklusjon, Ledelsens ansvar, Revisors oppgaver og plikter, ISA-ene, Vesentlig feilinformasjon, Profesjonell skepsis, Fortsatt drift

### 6.6 Generalforsamling Terminology

Valg av møteleder, Godkjenning av møteinnkallingen, Fastsettelse av honorarer, Valg av styremedlem, Vedtak, Enstemmige beslutninger

### 6.7 Fagforeninger og Arbeidsmiljø Terminology

**Labor relations:** Samarbeid med tillitsvalgte, Fagforeninger, Tariffavtale, Hovedavtalen, Tillitsvalgte, Verneombud

**Working environment (Årsberetning Section 5):** Arbeidsmiljø, HMS (Helse, Miljø og Sikkerhet), Sykefravær, Likestilling, Diskriminering, Personskader, Ytre miljø

**Audit context:** A-melding (payroll reporting system), Feriepengegrunnlag, Skyldig arbeidsgiveravgift, Skattetrekk

**Sustainability (N_ESG / IFRS S2):** Bærekraftsrapportering, Dobbel vesentlighet (double materiality), Impact materiality, Financial materiality, Klimarisiko, Sosiale forhold

---

## 7. Validation Signals from Note Content

Cross-checks derivable from extracted note data:

| Check | Source A | Source B | Validation |
|-------|----------|----------|------------|
| Payroll vs headcount | Lønnskostnader sum | Antall årsverk | If årsverk=0 then lønnskostnader should be 0 or near-0 |
| Equity opening | Egenkapital Pr. 01.01 | Prior year Egenkapital Pr. 31.12 | Must match |
| Equity movement | Årsresultat in EK note | Årsresultat in resultatregnskap | Must match |
| Tax payable | Betalbar skatt in tax note | Betalbar skatt in balanse | Must match |
| Fixed asset book value | Bokført verdi in anleggsmidler note | Varige driftsmidler in balanse | Must match |
| Pledged assets | Pantstillelser amount | Must ≤ relevant asset category total | |
| Inventory | Sum in varelager note | Varelager in balanse | Must match |
| Bundne midler | Skattetrekksmidler in bank note | Must ≤ Bankinnskudd in balanse | |
| Bundne vs skyldig skattetrekk | Bundne skattetrekksmidler | Skyldig skattetrekk (HAJ Holding pattern) | Bundne midler should ≥ skyldig skattetrekk |
| Pantsatt bankinnskudd | Pantsatt bankinnskudd in pantstillelser note | Bankinnskudd in balanse | Pledged deposits must ≤ total deposits |
| Intercompany net | Fordring konsern - Gjeld konsern | Net intercompany position | Consistency check |
| OTP obligation | Antall ansatte > 0 | OTP statement | If employees > 0, OTP should be declared |
| Feriepenger vs lønn | Skyldige feriepenger in annen kortsiktig gjeld note | Lønnskostnader sum | Feriepenger typically ~12% of gross salary |
| Personnel chain | Daglig leder employment disclosure | Årsverk count | If "ansatt i [other entity]" then 0 årsverk is valid |

---

## 8. Identified Data Quality Issues in Source Material

| Entity | Issue | Description |
|--------|-------|-------------|
| Hofsfossveien 3 | Antatt levetid | "2016" listed as economic life for buildings — appears to be acquisition year, not useful life in years |
| TH DRIPT | Number format | Amounts as "1102.00" — decimal format inconsistent with Norwegian standards, likely legacy system |
| Mosaique Headhunting | Equity table | Sum egenkapital shows 169,531 at both opening and closing despite positive årets resultat of 80,007 — arithmetic does not reconcile as presented |
| DANINOR AS | Fordringer sign | "Fordringer på personlige eiere: -1,022,566" — negative receivable suggests this is actually a liability |
| Camilla Lyngstad | Tax note columns | Deferred tax table uses "01.01." and "09.11." as column headers instead of year — entity-specific liquidation date |
| ROTO DRILL AS | Duplicate notes | Regnskapsprinsipper appears twice with minor textual differences between Part 1 and Part 2 |
| Empty templates | Multiple entities | PDF contains standard headings but no financial data — blank template artifacts |
| FSL HOLDING AS | Partial completion | Mix of filled and unfilled boilerplate sections — inconsistent data quality |

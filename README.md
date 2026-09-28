# C-MapReduce: Map-Reduce Framework in C

Un'implementazione del paradigma Map-Reduce sviluppata in C per sistemi operativi conformi allo standard POSIX. Il framework permette di suddividere, elaborare in parallelo e aggregare flussi di dati strutturati in coppie chiave/valore.

Sviluppato nell'ambito del corso di laurea presso l'Universita di Pisa, il codice separa il motore di esecuzione interno dalle funzioni di calcolo fornite dall'utente.

---

## Architettura del Sistema

Il framework segue le fasi classiche del modello Map-Reduce:

1. Map: I dati in ingresso vengono elaborati in parallelo da worker indipendenti, producendo coppie intermedie (chiave, valore).
2. Shuffle e Sort: Le coppie intermedie vengono raggruppate e ordinate per chiave, preparando l'input per i processi di riduzione.
3. Reduce: I valori associati a ciascuna chiave univoca vengono combinati per produrre i risultati definitivi.

Flusso di esecuzione:

    [Input] -> [Fase di Map] -> [Shuffle & Sort] -> [Fase di Reduce] -> [Output]

---

## Componenti Principali

- Core Engine: Motore di coordinamento dei task e sincronizzazione dei processi.
- API Dedicata: Interfaccia per la registrazione delle funzioni di Map e Reduce personalizzate (mr.h).
- Gestione della Memoria: Controllo esplicito delle risorse per limitare l'uso di memoria e prevenire leak.
- Utility di Supporto: Funzioni per il parsing delle stringhe e la gestione delle strutture dati ausiliarie.
- Esempi di Utilizzo:
  - Word Count: Conteggio delle occorrenze delle parole all'interno di file di testo.
  - Print Output: Formattazione standard dell'output elaborato.
- Suite di Test: Test per la verifica del corretto funzionamento delle componenti.

---

## Struttura della Repository

```text
├── include/       Header con definizioni di tipi e API pubbliche (mr.h)
├── src/           Sorgenti del framework e funzioni di utilita
├── examples/      Esempi applicativi (Word Count, Print Output)
├── tests/         Test di unita e integrazione
├── docs/          Documentazione tecnica del progetto in formato PDF
└── Makefile       File di configurazione per la compilazione
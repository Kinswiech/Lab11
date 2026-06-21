# LAB11 

# Zadanie 1
Do zrealizowania zadania wykorzystałam kontener Docker z obrazem Apache Spark 3.5.5, dzięki czemu uniknęłam problemów z konfigurają Hadoop oraz winults w Windowsie. 
<img width="870" height="257" alt="Zrzut ekranu 2026-06-21 233602" src="https://github.com/user-attachments/assets/2ce64219-ba03-4ee1-92c2-4775569ca3bf" />


# Zadanie 2
Przygotowałam folder wyjściowy do którego dodawane były pliki csv zawierające dane zdarzeń. Następnie dane zostały wczytane prze

```python
spark.readStream.csv(...)
```

Kolumna czasu przekonwertowałam do typu `timestamp`.

Sprawdziłam również, że utworzony DataFrame jest strumieniowy.

<img width="363" height="168" alt="Zrzut ekranu 2026-06-21 233834" src="https://github.com/user-attachments/assets/afcd9d8b-d7be-401b-b758-ff45cce75cc0" />


# Zadanie 3
Na danych wykonałam następujące transformacje:

- filtrowanie rekordów z pustymi wartościami
- filtrowanie statusu `paid`
- konwersję kolumny czasu do typu `timestamp`
- wybór wymaganych kolumn

Następnie wykonałam agregację danych według kategorii, czego wyniki były wyświetlane bezpośrednio w terminalu.
Podczas działania aplikacji generator automatycznie dodawał nowe pliki CSV do katalogu wejściowego. Spark wykrywał nowe dane bez konieczności restartu programu.

<img width="664" height="678" alt="Zrzut ekranu 2026-06-21 233853" src="https://github.com/user-attachments/assets/552f3607-2910-4f4c-874e-fa09dcec2fe8" />
<img width="667" height="860" alt="Zrzut ekranu 2026-06-21 233913" src="https://github.com/user-attachments/assets/51d63abd-24c8-4355-8437-4c0d1a135ade" />


# Zadanie 4 

W celu analizy danych w czasie zastosowałam:

```python
withWatermark("event_time", "10 minutes")
```

oraz okna czasowe:

```python
window(col("event_time"), "10 minutes")
```

Dzięki czemu było grupowanie zdarzeń według przedziałów czasowych

Przetestowałam:
- dane przychodzące w poprawnej kolejności
- dane opóźnione
- różne kategorie oraz wartości liczbowe

## Czas zdarzenia a czas przetwarzania
Czas zdarzenia to czas rzeczywistego wystąpienia zdarzenia zapisany w danych źródłowych, czas przetwarzania to moment w którym Spark otrzymał i przetworzył dane.

## Watermarking
Watermarking określa maksymalne opóźnienie akceptowane przez system dla danych przychodzących z opóźnieniem.



# Zadanie 5 – Zapis wyników i checkpointing
Wyniki przetwarzania były zapisywane do plików CSV.
Dodatkowo zastosowałam checkointing tak, żeby po zatrzymaniu i ponownym uruchomieniu aplikacji Spark wykorzystywał zapisany stan przetwarzania, dzięki czemu wcześniej przetworzone dane nie były analizowane ponownie.


## Różnice między checkpointingiem a zwykłym zapisem wyników

Zwykły zapis wyników przechowuje jedynie rezultaty przetwarzania, a checkpointing przechwuje stan zapytania strumieniowego, informacje o przetworzonych danych oraz postęp wykonywania.

<img width="532" height="629" alt="Zrzut ekranu 2026-06-21 234020" src="https://github.com/user-attachments/assets/326cddf6-03c9-4d46-aad4-d7c32e9b6bb9" />


## Tryby wyjścia:

- Append - do wyniku dopisywane są wyłącznie nowe rekordy.
- Update - aktualizowane są wyłącznie rekordy, które zmieniły się od poprzedniego uruchomienia mikroserii.
- Complete - przy każdej aktualizacji wyświetlany jest pełny wynik agregacji.

## Batch Processing a Streaming Processing
Batch processing polega na analizie wcześniej przygotowanego zbioru danych. Operacja wykonywana jest jednorazowo na całym zbiorze.
Streaming processing analizuje dane napływające w czasie rzeczywistym. Wyniki są aktualizowane automatycznie wraz z pojawianiem się nowych danych.


# Wnioski
Podczas wykonywania laboratorium nauczyłam się jak pracować z przetwarzaniem strumieniowym danych z Apache Spark. Przy wykonywaniu zadań dane były przetwarzane na bieżąco wraz z pojawieniem się nowych plików, 
w przeciwieństwie do poprzednich sprawozdań.


Najciekawszym elementem ćwiczenia było wykorzystanie Structured Streaming, ponieważ pozwala ono budować aplikacje reagujące na dane w czasie rzeczywistym przy użyciu składni bardzo podobnej do zwykłych DataFrame. Dzięki temu przejście od przetwarzania wsadowego do strumieniowego jest stosunkowo proste.
Ćwiczenie pozwoliło mi również zrozumieć znaczenie czasu zdarzenia, czasu przetwarzania oraz mechanizmu watermarkingu, który umożliwia poprawną obsługę danych przychodzących z opóźnieniem. Dodatkowo poznałam działanie checkpointingu i jego rolę w zapewnianiu odporności aplikacji na restart lub awarie.


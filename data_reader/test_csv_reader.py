from data_reader.csv_reader import DelimitedSource


def test_csv_reader_parse():
    filepath = "./data_reader/test_data/test_data1.csv"
    delimiter=","
    encapsulator="|"
    encoding="utf-8"

    # Initialise the reader
    csv_reader = DelimitedSource(filepath, delimiter, encapsulator, encoding)

    # Consume the entire generator
    data = list(csv_reader.parse())
    assert data == [{'Pedal name': 'TS-808', 'Manufacturer': 'Ibanez', 'Type of effect': 'Overdrive'},
                    {'Pedal name': 'Timeline', 'Manufacturer': 'Strymon', 'Type of effect': 'Delay'},
                    {'Pedal name': 'BigSky', 'Manufacturer': 'Strymon', 'Type of effect': 'Reverb'}]


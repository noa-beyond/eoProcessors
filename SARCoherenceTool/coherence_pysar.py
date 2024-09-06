import pysar
import os
import re

def list_slc_files(directory):
    """ List all SLC files in the given directory based on a specific pattern in the filenames. """
    pattern = re.compile(r'\d{8}')  # Regex to find dates in filenames
    slc_files = {}
    for filename in os.listdir(directory):
        if filename.endswith('.slc'):
            match = pattern.search(filename)
            if match:
                date = match.group(0)
                slc_files[date] = os.path.join(directory, filename)
    return slc_files

def create_pairs(slc_files):
    """ Create pairs of SLC files based on their dates. """
    sorted_dates = sorted(slc_files.keys())
    pairs = []
    for i in range(len(sorted_dates) - 1):
        pairs.append((slc_files[sorted_dates[i]], slc_files[sorted_dates[i + 1]]))
    return pairs

def preprocess_and_calculate_coherence(pair):
    """ Preprocess SLC files and calculate coherence between them. """
    # Load the SLC files
    slc1 = pysar.load_slc(pair[0])
    slc2 = pysar.load_slc(pair[1])

    # Preprocessing steps (hypothetical functions)
    slc1 = pysar.speckle_filter(slc1)
    slc2 = pysar.speckle_filter(slc2)
    slc1 = pysar.apply_orbit_file(slc1)
    slc2 = pysar.apply_orbit_file(slc2)

    # Calculate coherence
    coherence_map = pysar.calculate_coherence(slc1, slc2)
    return coherence_map

def main():
    directory = '/path/to/slc/files'  # Set the path to your SLC files
    slc_files = list_slc_files(directory)
    pairs = create_pairs(slc_files)

    for pair in pairs:
        coherence_map = preprocess_and_calculate_coherence(pair)
        print(f'Coherence between {pair[0]} and {pair[1]} calculated.')

if __name__ == '__main__':
    main()

import numpy as np
import matplotlib

matplotlib.use('TkAgg')
from fastai.text import *
from .message_utils import *
import sys
sys.excepthook = sys.__excepthook__  # See https://groups.io/g/insync/topic/13778827?p=,,,20,0,0,0::recentpostdate%2Fsticky,,,20,2,0,13778827
import json

import traceback
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_model(itos_filename, classifier_filename):
    """Load the classifier and int to string mapping
  
    Args:
        itos_filename (str): The filename of the int to string mapping file (usually called itos.pkl)
        classifier_filename (str): The filename of the trained classifier
  
    Returns:
        string to int mapping, trained classifer model
    """

    # load the int to string mapping file
    itos = pickle.load(Path(itos_filename).open('rb'))
    # turn it into a string to int mapping (which is what we need)
    stoi = collections.defaultdict(lambda: 0, {str(v): int(k) for k, v in enumerate(itos)})

    # these parameters aren't used, but this is the easiest way to get a model
    bptt, em_sz, nh, nl = 70, 400, 1150, 3
    dps = np.array([0.4, 0.5, 0.05, 0.3, 0.4]) * 0.5
    num_classes = 2  # this is the number of classes we want to predict
    vs = len(itos)

    model = get_rnn_classifer(bptt, 20 * 70, num_classes, vs, emb_sz=em_sz, n_hid=nh, n_layers=nl, pad_token=1,
                              layers=[em_sz * 3, 50, num_classes], drops=[dps[4], 0.1],
                              dropouti=dps[0], wdrop=dps[1], dropoute=dps[2], dropouth=dps[3])

    # load the trained classifier
    model.load_state_dict(torch.load(classifier_filename, map_location=lambda storage, loc: storage))

    # put the classifier into evaluation mode
    model.reset()
    model.eval()

    return stoi, model


def softmax(x):
    '''
    Numpy Softmax, via comments on https://gist.github.com/stober/1946926

    >>> res = softmax(np.array([0, 200, 10]))
    >>> np.sum(res)
    1.0
    >>> np.all(np.abs(res - np.array([0, 1, 0])) < 0.0001)
    True
    >>> res = softmax(np.array([[0, 200, 10], [0, 10, 200], [200, 0, 10]]))
    >>> np.sum(res, axis=1)
    array([ 1.,  1.,  1.])
    >>> res = softmax(np.array([[0, 200, 10], [0, 10, 200]]))
    >>> np.sum(res, axis=1)
    array([ 1.,  1.])
    '''
    if x.ndim == 1:
        x = x.reshape((1, -1))
    max_x = np.max(x, axis=1).reshape((-1, 1))
    exp_x = np.exp(x - max_x)
    return exp_x / np.sum(exp_x, axis=1).reshape((-1, 1))

def predict_text_sentiment(stoi, model, text):
    """Do the actual prediction on the text using the
        model and mapping files passed
    """

    # prefix text with tokens:
    #   xbos: beginning of sentence
    #   xfld 1: we are using a single field here
    input_str = 'xbos xfld 1 ' + text

    # predictions are done on arrays of input.
    # We only have a single input, so turn it into a 1x1 array
    texts = [input_str]

    # tokenize using the fastai wrapper around spacy
    tok = Tokenizer().proc_all_mp(partition_by_cores(texts))

    # turn into integers for each word
    encoded = [stoi[p] for p in tok[0]]

    # we want a [x,1] array where x is the number
    #  of words inputted (including the prefix tokens)
    ary = np.reshape(np.array(encoded), (-1, 1))

    # turn this array into a tensor
    tensor = torch.from_numpy(ary)

    # wrap in a torch Variable
    variable = Variable(tensor)

    # do the predictions
    predictions = model(variable)

    # convert back to numpy
    numpy_preds = predictions[0].data.numpy()

    return softmax(numpy_preds[0])[0]

def predict_json_record(
        json_record, stoi, model,
        itos_file_path, trained_classifier_file_path, input_data_file_path
):
    text = json_record['data']['text']
    scores = predict_text_sentiment(stoi, model, text)
    delta_ts = datetime.utcnow() - datetime(1970, 1, 1)
    prediction_processed_ts_ms = int((delta_ts.days * 24 * 60 * 60 + delta_ts.seconds) * 1000 + delta_ts.microseconds / 1000.0)

    json_record['predictions'] = {
        'model': trained_classifier_file_path,
        'context': [{'stoi': itos_file_path, 'inputDataset': input_data_file_path}],
        'predictionProcessedTsMs': prediction_processed_ts_ms,
        'values': {
            'bear_sentiment': float(scores[0]),
            'bull_sentiment': float(scores[1]),
        }
    }

    return json_record

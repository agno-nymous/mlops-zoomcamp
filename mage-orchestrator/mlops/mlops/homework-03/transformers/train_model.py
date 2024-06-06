from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LinearRegression

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    categorical = ['PULocationID', 'DOLocationID']
    numerical = ['trip_distance']
    target = "duration"

    #fit dict vectorizer
    data_dict = data[categorical].to_dict(orient="records")

    dv = DictVectorizer()
    X = dv.fit_transform(data_dict)
    y = data[target].values

    lr = LinearRegression()
    lr.fit(X, y)

    print(lr.intercept_)

    return dv, lr
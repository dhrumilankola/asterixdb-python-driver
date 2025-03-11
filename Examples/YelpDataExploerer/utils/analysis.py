# utils/analysis.py
from textblob import TextBlob
import matplotlib.pyplot as plt
from wordcloud import WordCloud

def analyze_sentiment(text):
    analysis = TextBlob(text)
    if analysis.sentiment.polarity > 0:
        return 'Positive'
    elif analysis.sentiment.polarity == 0:
        return 'Neutral'
    else:
        return 'Negative'

def generate_wordcloud(text):
    wordcloud = WordCloud(width=800, height=400, background_color='white',
                          max_words=200, contour_width=3, contour_color='steelblue').generate(text)
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.imshow(wordcloud, interpolation='bilinear')
    ax.axis('off')
    return fig

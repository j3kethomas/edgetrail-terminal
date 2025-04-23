from dash import Dash,html
from dash_bootstrap_components.themes import BOOTSTRAP

def main():
    app = Dash(__name__, external_stylesheets= [BOOTSTRAP])
    app.layout = html.H1(
        children = 'Welcome to Edgetrail Terminal'
    )
    app.run()
    
if __name__ == "__main__":
    main() 
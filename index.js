const express = require('express');
const app = express();
const port = 8080;
const { Client } = require('@elastic/elasticsearch');
const client = new Client({
    node: 'http://localhost:9200/',
    auth: {
        apiKey: 'qESQyllOQSWCkKmHNGBRfg'
    }
})


app.use(express.json());

app.listen(port, async () => {
    try {
        // Vérifier la connexion à Elasticsearch au démarrage du serveur
        await client.ping();
        const resp = await client.info()
        console.log('Connected to Elasticsearch');
        console.log(resp)


    } catch (error) {
        console.error('Error connecting to Elasticsearch:', error);
    }
    console.log(`Server listening at http://localhost:${port}`);
});

app.get('/', (req, res) => {
    try {
        res.send('Hello World!')
    } catch (error) {
        res.send("Error")
    }
})

app.get('/create', async (req, res) => {
    const result = await client.helpers.bulk({
        datasource: dataset,
        onDocument: (doc) => ({ index: { _index: 'index_name' } }),
    });
    res.send(result)
    console.log(result);
})

app.get('/search', async (req, res) => {
    // Let's search!
    const searchResult = await client.search({
        index: 'index_name',
        q: 'snow'
    });
    res.send(searchResult)
    console.log(searchResult.hits.hits);
})

const dataset = [
    { "name": "Snow Crash", "author": "Neal Stephenson", "release_date": "1992-06-01", "page_count": 470 },
    { "name": "Revelation Space", "author": "Alastair Reynolds", "release_date": "2000-03-15", "page_count": 585 },
    { "name": "1984", "author": "George Orwell", "release_date": "1985-06-01", "page_count": 328 },
    { "name": "Fahrenheit 451", "author": "Ray Bradbury", "release_date": "1953-10-15", "page_count": 227 },
    { "name": "Brave New World", "author": "Aldous Huxley", "release_date": "1932-06-01", "page_count": 268 },
    { "name": "The Handmaid's Tale", "author": "Margaret Atwood", "release_date": "1985-06-01", "page_count": 311 },
];






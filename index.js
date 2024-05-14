const express = require('express');
const app = express();
const port = 8080;
const { Client } = require('@elastic/elasticsearch');
const client = new Client({
    node: 'https://my-deployment-f6a57b.es.us-central1.gcp.cloud.es.io',
    auth: {
        apiKey: 'OUp3cGRvOEJiRmpmRS10S0JkajI6QlNTMEt2Q1ZSYU90TWF3MGlwOHBXQQ=='
    }
})
const fs = require('fs');
const csv = require('csv-parser');
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
        res.send("Error : " + errors)
    }
})

app.get('/create', async (req, res) => {
    let result
    let json = []
    await fs.createReadStream("./Donnee/accessories.csv")
        .pipe(csv({ separator: ',' }))
        .on('data', (data) => {
            json.push(data)
        })
        .on('end', async () => {
            fs.readdir("./Donnee", (err, files) => {
                if (err) {
                    console.error(err)
                    return;
                }
                files.forEach(async file => {
                    if (file != undefined) {
                        result = await client.helpers.bulk({
                            datasource: json,
                            onDocument: (doc) => ({ index: { _index: file.replace(".csv", "") } }),
                        });
                    }
                });
            })
            res.send(result)
        })
    })

    app.get('/search', async (req, res) => {
        const searchResult = await client.search({
            index: 'bags',
            q: "Able beak",
            fields: ["Source Notes", "Source", "Name"],
            default_operator: "OR"
        });
        res.send(searchResult)
        console.log(searchResult.hits.hits);
    })

    app.get('/all', async (req, res) => {
        const allIndex = await client.search({
            size: 0,
            query: {
                bool: {
                    must_not: {
                        prefix: {
                            _index: "."
                        }
                    }
                }
            },
            aggs: {
                indices: {
                    terms: {
                        field: "_index",
                        size: 1000
                    }
                }
            }
        })
        res.send(allIndex)
    })

    app.get('/aggregate', async (req, res) => {
        const aggregateResult = await client.search({
            index: '',
            query: {
                match: {
                    name: 'beak'
                }
            },
            aggregations: {
                by_price: {
                    terms:{
                        field: "Buy",
                        size: 100
                    }
                }
            }
        })
        res.send(aggregateResult)
    })

  
package httpserver

import (
	"encoding/json"
	"log"
	"net/http"

	"router-ingest-go/internal/kafka"
	"router-ingest-go/internal/model"
)

func StartHTTPServer(port string, producer *kafka.Producer) {
	http.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		var m model.Metric
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := producer.Produce(m); err != nil {
			http.Error(w, "failed to produce metric", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	})

	log.Println("HTTP server running on port", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

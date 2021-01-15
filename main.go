package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	runtime "github.com/aws/aws-lambda-go/lambda"
	"github.com/dexterorion/ws-to-s3-lambda/s3"
	"github.com/dexterorion/ws-to-s3-lambda/soap"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"strings"
	"time"
)

var (
	pagamentosFile   = os.Getenv("FILE_PAGAMENTOS")
	saidasFile       = os.Getenv("FILE_SAIDAS")
	credenciadosFile = os.Getenv("FILE_CREDENCIADOS")

	pagamentosBucket   = os.Getenv("BUCKET_PAGAMENTOS")
	saidasBucket       = os.Getenv("BUCKET_SAIDAS")
	credenciadosBucket = os.Getenv("BUCKET_CREDENCIADOS")

	filial = os.Getenv("FILIAL")

	ws     string = os.Getenv("WS")
	action string = os.Getenv("action")

	log *zap.Logger
)

func handleRequest(ctx context.Context, event events.SQSEvent) (string, error) {
	log, _ = zap.NewProduction()
	defer log.Sync()

	log.Info("Starting lambda function....")

	var err error
	var startDate, endDate time.Time
	endDate = time.Now()
	startDate = endDate.Add(-1600 * time.Millisecond)
	err = getPagamentos(startDate, endDate)
	if err != nil {
		log.Error("Error getting pagamentos")
		return "", err
	}

	err = getSaidas(startDate, endDate)
	if err != nil {
		log.Error("Error getting saidas")
		return "", err
	}

	err = getCredenciados()
	if err != nil {
		log.Error("Error getting credenciados")
		return "", err
	}

	return "ok", nil
}

func readfile(filepath string, startDate, endDate time.Time) ([]byte, error) {
	data, err := ioutil.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("error opening file [%s]: [%s]", filepath, err.Error())
	}

	payload := string(data)
	payload = strings.ReplaceAll(payload, ":start_date", startDate.Format("2006-01-02T15:04:05"))
	payload = strings.ReplaceAll(payload, ":end_date", endDate.Format("2006-01-02T15:04:05"))
	payload = strings.ReplaceAll(payload, ":filial", filial)

	response, err := soap.SoapCall(ws, action, []byte(payload))
	if err != nil {
		return nil, fmt.Errorf("error requesting service: [%s]", err.Error())
	}

	return response, nil
}

func saveResponse(bucket, filename string, body []byte) error {
	return s3.Upload(bucket, filename, body)
}

func getPagamentos(startDate, endDate time.Time) error {
	log.Info(fmt.Sprintf("Getting pagamentos.... Start date: [%s]   ----    End date: [%s]", startDate.String(), endDate.String()))
	response, err := readfile(pagamentosFile, startDate, endDate)
	if err != nil {
		return err
	}

	responseFile := fmt.Sprintf("pagamentos-%s-%s.xml", startDate.String(), endDate.String())
	err = saveResponse(pagamentosBucket, responseFile, response)
	if err != nil {
		return err
	}

	log.Info("Getting pagamentos done successfully....")
	return nil
}

func getSaidas(startDate, endDate time.Time) error {
	log.Info(fmt.Sprintf("Getting saidas.... Start date: [%s]   ----    End date: [%s]", startDate.String(), endDate.String()))
	response, err := readfile(saidasFile, startDate, endDate)
	if err != nil {
		return err
	}

	responseFile := fmt.Sprintf("saidas-%s-%s.xml", startDate.String(), endDate.String())
	err = saveResponse(saidasBucket, responseFile, response)
	if err != nil {
		return err
	}

	log.Info("Getting saidas done successfully....")
	return nil
}

func getCredenciados() error {
	unused := time.Now()
	log.Info(fmt.Sprintf("Getting credenciados.... Start date: [%s]   ----    End date: [%s]", unused.String(), unused.String()))
	response, err := readfile(credenciadosFile, unused, unused)
	if err != nil {
		return err
	}

	responseFile := fmt.Sprintf("credenciados-%s-%s.xml", unused.String(), unused.String())
	err = saveResponse(credenciadosBucket, responseFile, response)
	if err != nil {
		return err
	}

	log.Info("Getting credenciados done successfully....")
	return nil
}

func main() {
	runtime.Start(handleRequest)
}

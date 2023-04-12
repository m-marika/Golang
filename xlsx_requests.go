package export

import (
	"fmt"
	"ngs-core/graph/model"
	"ngs-core/libs/uuid"
	"time"

	"github.com/friendsofgo/errors"
	"github.com/spf13/viper"
)

var (
	headInRus = map[string]string{
		"highPriority" : "Приоритет",
		"externalID" : "Номер заявки",
		"clientID" : "ID клиента",
		"accountName" : "Учетная запись",
		"gender" : "Пол",
		"age" : "Возраст",
		"labID" : "Лабораторный номер",
		"customer" : "Заказчик",
		"service" : "Услуга",
		"registeredAt" : "Поступление в лабораторию",
		"deliveryEstimation" : "Выдача по плану",
		"karyotypeFileCreatedAt" : "Готовность кариотипа",
		"deliveredAt" : "Выдача по факту",
		"state" : "Статус",
		"karyotypeRequired" : "Запрос кариотипа",
		"rawDataRequired" : "Отправлять FASTQ",
		"tubeCount" : "Кол-во пробирок",
		"tubedAt" : "Дата забора",
		"clinic" : "Клиника",
		"doctor" : "Врач",
		"type" : "Тип заявки",
		"createdAt" : "Дата создания",
		"updatedAt" : "Последнее изменение",
		"comments" : "Комментарий",
	}
)

type RequestsXlsxTemplateExporter struct {
	template     *XlsxTemplate
	requests     []*model.Request
	params 		 *model.ExportParams
	pathResolver xlsxTemplatePathResolver
}

func NewRequestsXlsxTemplateExporter(Requests []*model.Request, params *model.ExportParams) Exporter {
	result := RequestsXlsxTemplateExporter{}
	result.requests = Requests
	result.params = params
	result.pathResolver = newXlsxTemplatePathResolver(viper.GetString("xlsx_export_template_dir"))
	return &result
}

func (e *RequestsXlsxTemplateExporter) Export() (*ExportedData, error) {
	if len(e.requests) == 0 {
		return nil, errors.New("requests is empty")
	}

	request := e.requests[0]
	templatePath := e.pathResolver.resolveForRequest(request) //return "template.xlsx"
	template, err := NewXlsxTemplate(templatePath)
	if err != nil {
		return nil, err
	}

	var head []string
	for _, item := range e.params.Headers{
		head = append(head,  headInRus[*item])
	}

	err = template.XlsxLineWrite("Заявки", "A1", head)
	if err != nil {
		return nil, err
	}

	placeID := 2
	for _, item := range e.requests {
		commonFields, err := e.XLSXWrite(item)
		if err != nil {
			return nil, err
		}
		err = template.XlsxLineWrite("Заявки", fmt.Sprintf("A%d", placeID) ,commonFields)
		if err != nil {
			return nil, err
		}
		placeID += 1
	}

	id, _ := uuid.NewUUID()
	filename := fmt.Sprintf("request.%s.xlsx", time.Now().Format("2006_01_02T15_04_05"))
	result := ExportedData{
		ID:       id,
		Format:   XLSX,
		Filename: filename,
	}
	result.Data, err = template.Data()
	if err != nil {
		return nil, err
	}

	return &result, nil
}

func (e *RequestsXlsxTemplateExporter) XLSXWrite(request *model.Request) ([]string, error) {
commonFields := make([]string, len(e.params.Headers) )
	for index := range e.params.Headers {
		switch *e.params.Headers[index] {
		case "highPriority":
			commonFields[index] = e.safeBool(request.HighPriority)
		case "externalID":
			if (request.ExternalID != nil) {
				commonFields[index] = e.safeStr(request.ExternalID)
			}
		case "clientID":
			if (request.Client != nil) {
				commonFields[index] = e.safeStr(request.Client.ExternalID)
			}	
		case "gender":
			if (request.Client != nil) {
				commonFields[index] = e.safeGenger(&request.Client.Gender)
			}
		case "age":
			if (request.Client != nil) {
				commonFields[index] = e.safeInt(request.Client.Age)
			}
		case "labID":
			if (request.LabID != nil) {
				commonFields[index] = e.safeStr(request.LabID)
			}
		case "customer":
			if (request.Customer != nil) {
				commonFields[index] = e.safeStr(&request.Customer.Name)
			}	
		case "service":
			if (request.Service != nil) {
				commonFields[index] = e.safeStr(&request.Service.Name)
			}	
		case "registeredAt":
			if (request.RegisteredAt != nil) {
				commonFields[index] = InMoscowTimeWithFormat(request.RegisteredAt, "02.01.2006")
			}
		case "deliveryEstimation":
			if request.DeliveryEstimation != nil{
				commonFields[index] = InMoscowTimeWithFormat(request.DeliveryEstimation, "02.01.2006")
			}
		case "karyotypeFileCreatedAt":
			if request.KaryotypeFileCreatedAt != nil {
				commonFields[index] = InMoscowTimeWithFormat(request.KaryotypeFileCreatedAt, "02.01.2006")
			}
		case "deliveredAt":
			if request.DeliveredAt != nil {
				commonFields[index] = InMoscowTimeWithFormat(request.DeliveredAt, "02.01.2006")
			}
		case "state":
			if (request.State != nil) {
				commonFields[index] = e.safeStr(&request.State.Name)
			}
		case "accountName":
			if request.Client != nil && request.Client.AccountName != nil {
				commonFields[index] = e.safeStr(request.Client.AccountName)
			}
		case "karyotypeRequired":
			commonFields[index] = e.safeBool(request.KaryotypeRequired)
		case "rawDataRequired":
			commonFields[index] = e.safeBool(request.RawDataRequired)
		case "tubeCount":
			commonFields[index] = e.safeInt(request.TubeCount)
		case "tubedAt":
			if request.TubedAt != nil {
				commonFields[index] = InMoscowTimeWithFormat(request.TubedAt, "02.01.2006")
			}
		case "clinic":
			if request.Clinic != nil {
				commonFields[index] = e.safeStr(&request.Clinic.Name)
			}
		case "doctor":
			if request.Doctor != nil {
				commonFields[index] = e.safeDoctor(&request.Doctor.FirstName, &request.Doctor.LastName, request.Doctor.MiddleName)
			}
		case "type":
			if request.Type != nil {
				commonFields[index] = e.safeStr(&request.Type.Name)
			}
		case "createdAt":
			commonFields[index] = InMoscowTimeWithFormat(&request.CreatedAt, "02.01.2006 15:04")
		case "updatedAt":
			if request.UpdatedAt != nil {
				commonFields[index] = InMoscowTimeWithFormat(request.UpdatedAt, "02.01.2006 15:04")
			}
		case "comments":
			if request.Comments != nil {
				commonFields[index] = e.safeStr(request.Comments)
			}
		}

		if (index % (len(e.params.Headers) - 1) == 0) && (index != 0) {
			return commonFields, nil
		}
	}
	return nil, nil
}

func (e *RequestsXlsxTemplateExporter) safeStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func (e *RequestsXlsxTemplateExporter) safeInt( i *int) string {
	if i == nil || *i == -1 {
		return ""
	}
	
	return fmt.Sprintf("%d", *i)
}

func (e *RequestsXlsxTemplateExporter) safeBool( i bool) string {
	if (!i)  {
		return ""
	}
	
	return fmt.Sprintf("%t", i)
}

func (e *RequestsXlsxTemplateExporter) safeGenger( g *model.EGender) string {
	if g == nil || g.String() == "UNSPECIFIED" {
		return ""
	}

	if g.String() == "FEMALE"{
		return "Женский" 
	} else {
		return "Мужской"
	}
}

func (e *RequestsXlsxTemplateExporter) safeDoctor( n *string, l *string, m *string) string {

	if n == nil {
		return ""
	}

	if l == nil {
		if m == nil {
			return *n
		} else {
			return fmt.Sprintf("%s %s", *n, *m)
		}
	} 

	if m == nil {
		return fmt.Sprintf("%s %s", *l, *n)
	}

	return fmt.Sprintf("%s %s %s", *l, *n, *m)
}
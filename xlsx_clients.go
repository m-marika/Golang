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
	headRus = map[string]string{
		"externalID" : "Номер заявки",
		"customer" : "Заказчик",
		"accountName" : "Учетная запись",
		"fullName" : "ФИО",
		"gender" : "Пол",
		"birthday" : "Дата рождения",
		"nationality" : "Национальность",
		"phone" : "Телефон",
		"email" : "Email",
		"emailVerified" : "Подтверждение email",
		"createdAt" : "Дата создания",
		"updatedAt" : "Последнее изменение",
	}
)

type ClientsXlsxTemplateExporter struct {
	template     *XlsxTemplate
	clients     []*model.Client
	params 		 *model.ExportParams
	pathResolver xlsxTemplatePathResolver
}

func NewClientsXlsxTemplateExporter(Clients []*model.Client, params *model.ExportParams) Exporter {
	result := ClientsXlsxTemplateExporter{}
	result.clients = Clients
	result.params = params
	result.pathResolver = newXlsxTemplatePathResolver(viper.GetString("xlsx_export_template_dir"))
	return &result
}

func (e *ClientsXlsxTemplateExporter) Export() (*ExportedData, error) {
	if len(e.clients) == 0 {
		return nil, errors.New("clients is empty")
	}

	client := e.clients[0]
	templatePath := e.pathResolver.resolveForClient(client)
	template, err := NewXlsxTemplate(templatePath)
	if err != nil {
		return nil, err
	}

	var head []string
	for _, item := range e.params.Headers{
		head = append(head,  headRus[*item])
	}

	err = template.XlsxLineWrite("Клиенты", "A1", head)
	if err != nil {
		return nil, err
	}

	placeID := 2
	for _, item := range e.clients {
		commonFields, err := e.XLSXWrite(item)
		if err != nil {
			return nil, err
		}
		err = template.XlsxLineWrite("Клиенты", fmt.Sprintf("A%d", placeID) ,commonFields)
		if err != nil {
			return nil, err
		}
		placeID += 1
	}

	id, _ := uuid.NewUUID()
	filename := fmt.Sprintf("client.%s.xlsx", time.Now().Format("2006_01_02T15_04_05"))
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

func (e *ClientsXlsxTemplateExporter) XLSXWrite(client *model.Client) ([]string, error) {
	commonFields := make([]string, len(e.params.Headers) )
	for index := range e.params.Headers {
		switch *e.params.Headers[index] {
		case "externalID":
			if (client.ExternalID != nil) {
				commonFields[index] = e.safeStr(client.ExternalID)
			}	
		case "gender":
			commonFields[index] = e.safeGenger(&client.Gender)
		case "customer":
			if (client.Customer != nil) {
				commonFields[index] = e.safeStr(&client.Customer.Name)
			}		
		case "fullName":
			commonFields[index] = e.safeName(client.FirstName, client.LastName, client.MiddleName)
		case "birthday":
			if client.Birthday != nil{
				commonFields[index] = InMoscowTimeWithFormat(client.Birthday, "02.01.2006")
			}
		case "nationality":
			if client.Nationality != nil {
				commonFields[index] = e.safeStr(client.Nationality)
			}
		case "phone":
			if client.Phones != nil {
				commonFields[index] = e.safePhones(client.Phones)
			}
		case "email":
			if (client.Email != nil) {
				commonFields[index] = e.safeStr(client.Email)
			}
		case "accountName":
			if client.AccountName != nil {
				commonFields[index] = e.safeStr(client.AccountName)
			}
		case "emailVerified":
			commonFields[index] = e.safeBool(client.EmailVerified)
		case "updatedAt":
			if client.UpdatedAt != nil {
				commonFields[index] = InMoscowTimeWithFormat(client.UpdatedAt, "02.01.2006 15:04")
			}
		case "createdAt":
			commonFields[index] = InMoscowTimeWithFormat(&client.CreatedAt, "02.01.2006 15:04")
		}
	
		if (index % (len(e.params.Headers) - 1) == 0) && (index != 0) {
			return commonFields, nil
		}
	}
	return nil, nil
}

func (e *ClientsXlsxTemplateExporter) safeStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func (e *ClientsXlsxTemplateExporter) safeBool( i bool) string {
	if (!i)  {
		return ""
	}
	
	return fmt.Sprintf("%t", i)
}

func (e *ClientsXlsxTemplateExporter) safeGenger( g *model.EGender) string {
	if g == nil || g.String() == "UNSPECIFIED" {
		return ""
	}

	if g.String() == "FEMALE"{
		return "Женский" 
	} else {
		return "Мужской"
	}
}

func (e *ClientsXlsxTemplateExporter) safeName( n *string, l *string, m *string) string {
	if n == nil && l == nil && m == nil {
		return ""
	}

	if l == nil {
		if n == nil {
			return *m
		} else {
			if m == nil{
				return *n
			} else {
				return fmt.Sprintf("%s %s", *n, *m)
			}
		}	
	} else {
		if m == nil {
			if n == nil {
				return *l
			} else {
				return fmt.Sprintf("%s %s", *l, *n)
			}
		} else {
			if n == nil {
				return fmt.Sprintf("%s %s", *l, *m)
			} else {
				return fmt.Sprintf("%s %s %s", *l, *n, *m)
			}
		}
	}
}

func (e *ClientsXlsxTemplateExporter) safePhones(p []string) string {
	if len(p) == 0 {
		return ""
	}

	var result string
	if len(p) > 1 {
		for _,v := range p {
			result = result + fmt.Sprintln(v)
		}
		return result
	}
	
	return fmt.Sprintf(p[0])
}
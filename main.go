package main

import (
	"context"
	"fmt"

	"github.com/Sugi275/oci-dataintegration-taskrun-sample/loglib"
	"github.com/oracle/oci-go-sdk/common"
	"github.com/oracle/oci-go-sdk/common/auth"
	"github.com/oracle/oci-go-sdk/dataintegration"

	"github.com/google/uuid"
)

func main() {
	loglib.InitSugar()
	defer loglib.Sugar.Sync()

	fmt.Println("Process Start!")

	dataIntegrationClient, err := getDataIntegrationClient()
	if err != nil {
		loglib.Sugar.Error(err)
		return
	}

	createTaskRun(dataIntegrationClient)

	fmt.Println("Process End!")
}

func getDataIntegrationClient() (client dataintegration.DataIntegrationClient, err error) {
	var dataIntegrationClient dataintegration.DataIntegrationClient

	provider, err := auth.InstancePrincipalConfigurationProvider()
	if err != nil {
		loglib.Sugar.Error(err)
		return dataIntegrationClient, err
	}

	dataIntegrationClient, err = dataintegration.NewDataIntegrationClientWithConfigurationProvider(provider)
	if err != nil {
		loglib.Sugar.Error(err)
		return dataIntegrationClient, err
	}

	return dataIntegrationClient, nil
}

func createTaskRun(dataIntegrationClient dataintegration.DataIntegrationClient) {
	var rootObjectValueInterface interface{}
	rootObjectValueInterface = map[string]interface{}{
		"dataFormat": map[string]interface{}{
			"formatAttribute": map[string]interface{}{
				"delimiter":       ",",
				"encoding":        "UTF-8",
				"escapeCharacter": "\\",
				"hasHeader":       "true",
				"modelType":       "CSV_FORMAT",
				"quoteCharacter":  "\"",
				"timestampFormat": "yyyy-MM-dd HH:mm:ss.SSS",
			},
			"type": "CSV",
		},
		"entity": map[string]interface{}{
			"key":          "dataref:4081caef-488a-440b-9d76-871912c4d3f0/input/FILE_ENTITY:paramtest02.csv",
			"modelType":    "FILE_ENTITY",
			"objectStatus": 1,
		},
		"modelType": "ENRICHED_ENTITY",
	}

	// 上記複雑な変数は、次のJSONオブジェジェクトを生成するために必要なもの。OCI SDK の インターフェース上、このような指定方法となる
	//      {
	//         "dataFormat": {
	//             "formatAttribute": {
	//                 "delimiter": ",",
	//                 "encoding": "UTF-8",
	//                 "escapeCharacter": "\\",
	//                 "hasHeader": "true",
	//                 "modelType": "CSV_FORMAT",
	//                 "quoteCharacter": "\"",
	//                 "timestampFormat": "yyyy-MM-dd HH:mm:ss.SSS"
	//             },
	//             "type": "CSV"
	//         },
	//         "entity": {
	//             "key": "dataref:4081caef-488a-440b-9d76-871912c4d3f0/input/FILE_ENTITY:paramtest03.csv",
	//             "modelType": "FILE_ENTITY",
	//             "objectStatus": 1
	//         },
	//         "modelType": "ENRICHED_ENTITY"
	//     }
	//

	parameterValue := map[string]dataintegration.ParameterValue{
		"INPUT_OBJECT_NAME": {RootObjectValue: &rootObjectValueInterface},
	}

	configProvider := dataintegration.CreateConfigProvider{
		Bindings: parameterValue,
	}

	uuidString, err := uuid.NewRandom()
	if err != nil {
		loglib.Sugar.Error(err)
		return
	}

	createTaskRunDetails := dataintegration.CreateTaskRunDetails{
		Key:              common.String(uuidString.String()),                                                                      // Task Run Key に、UUID を与えて、一意の文字列にする
		RegistryMetadata: &dataintegration.RegistryMetadata{AggregatorKey: common.String("f00c0f5c-da6d-4756-9fad-05b30840b181")}, // Application に Publish している Task の Task Key を指定
		ConfigProvider:   &configProvider,
	}

	createTaskRequest := dataintegration.CreateTaskRunRequest{
		WorkspaceId:          common.String("ocid1.disworkspace.oc1.ap-tokyo-1.amaaaaaassl65iqa4726obzimlzokp4p2tscrb3ykye2xin4ltwdnf5ioh4q"),
		ApplicationKey:       common.String("2848c0c8-8400-4ac2-8d75-bcf38ad7c9b2"),
		CreateTaskRunDetails: createTaskRunDetails,
	}

	response, err := dataIntegrationClient.CreateTaskRun(context.Background(), createTaskRequest)
	if err != nil {
		loglib.Sugar.Error(err)
		return
	}

	fmt.Println(response)
}

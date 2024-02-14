package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/Shopify/sarama"
)

type NetworkTrafficData struct {
	Unnamed0                string  `json:"Unnamed: 0"`
	FlowID                  string  `json:"Flow ID"`
	SourceIP                string  `json:"Source IP"`
	SourcePort              int     `json:"Source Port"`
	DestinationIP           string  `json:"Destination IP"`
	DestinationPort         int     `json:"Destination Port"`
	Protocol                int     `json:"Protocol"`
	Timestamp               string  `json:"Timestamp"`
	FlowDuration            int64   `json:"Flow Duration"`
	TotalFwdPackets         int     `json:"Total Fwd Packets"`
	TotalBackwardPackets    int     `json:"Total Backward Packets"`
	TotalLengthOfFwdPackets int64   `json:"Total Length of Fwd Packets"`
	TotalLengthOfBwdPackets int64   `json:"Total Length of Bwd Packets"`
	FwdPacketLengthMax      int     `json:"Fwd Packet Length Max"`
	FwdPacketLengthMin      int     `json:"Fwd Packet Length Min"`
	FwdPacketLengthMean     float64 `json:"Fwd Packet Length Mean"`
	FwdPacketLengthStd      float64 `json:"Fwd Packet Length Std"`
	BwdPacketLengthMax      int     `json:"Bwd Packet Length Max"`
	BwdPacketLengthMin      int     `json:"Bwd Packet Length Min"`
	BwdPacketLengthMean     float64 `json:"Bwd Packet Length Mean"`
	BwdPacketLengthStd      float64 `json:"Bwd Packet Length Std"`
	FlowBytesPerSecond      float64 `json:"Flow Bytes/s"`
	FlowPacketsPerSecond    float64 `json:"Flow Packets/s"`
	FlowIATMean             float64 `json:"Flow IAT Mean"`
	FlowIATStd              float64 `json:"Flow IAT Std"`
	FlowIATMax              int64   `json:"Flow IAT Max"`
	FlowIATMin              int64   `json:"Flow IAT Min"`
	FwdIATTotal             int64   `json:"Fwd IAT Total"`
	FwdIATMean              float64 `json:"Fwd IAT Mean"`
	FwdIATStd               float64 `json:"Fwd IAT Std"`
	FwdIATMax               int64   `json:"Fwd IAT Max"`
	FwdIATMin               int64   `json:"Fwd IAT Min"`
	BwdIATTotal             int64   `json:"Bwd IAT Total"`
	BwdIATMean              float64 `json:"Bwd IAT Mean"`
	BwdIATStd               float64 `json:"Bwd IAT Std"`
	BwdIATMax               int64   `json:"Bwd IAT Max"`
	BwdIATMin               int64   `json:"Bwd IAT Min"`
	FwdPSHFlags             int     `json:"Fwd PSH Flags"`
	BwdPSHFlags             int     `json:"Bwd PSH Flags"`
	FwdURGFlags             int     `json:"Fwd URG Flags"`
	BwdURGFlags             int     `json:"Bwd URG Flags"`
	FwdHeaderLength         int     `json:"Fwd Header Length"`
	BwdHeaderLength         int     `json:"Bwd Header Length"`
	FwdPacketsPerSecond     float64 `json:"Fwd Packets/s"`
	BwdPacketsPerSecond     float64 `json:"Bwd Packets/s"`
	MinPacketLength         int     `json:"Min Packet Length"`
	MaxPacketLength         int     `json:"Max Packet Length"`
	PacketLengthMean        float64 `json:"Packet Length Mean"`
	PacketLengthStd         float64 `json:"Packet Length Std"`
	PacketLengthVariance    float64 `json:"Packet Length Variance"`
	FINFlagCount            int     `json:"FIN Flag Count"`
	SYNFlagCount            int     `json:"SYN Flag Count"`
	RSTFlagCount            int     `json:"RST Flag Count"`
	PSHFlagCount            int     `json:"PSH Flag Count"`
	ACKFlagCount            int     `json:"ACK Flag Count"`
	URGFlagCount            int     `json:"URG Flag Count"`
	CWEFlagCount            int     `json:"CWE Flag Count"`
	ECEFlagCount            int     `json:"ECE Flag Count"`
	DownUpRatio             int     `json:"Down/Up Ratio"`
	AveragePacketSize       float64 `json:"Average Packet Size"`
	AvgFwdSegmentSize       float64 `json:"Avg Fwd Segment Size"`
	AvgBwdSegmentSize       float64 `json:"Avg Bwd Segment Size"`
	FwdHeaderLength1        int     `json:"Fwd Header Length.1"`
	FwdAvgBytesBulk         int     `json:"Fwd Avg Bytes/Bulk"`
	FwdAvgPacket
    
    
	FwdAvgBulkRate          int     `json:"Fwd Avg Bulk Rate"`
	BwdAvgBytesBulk         int     `json:"Bwd Avg Bytes/Bulk"`
	BwdAvgPacketsBulk       int     `json:"Bwd Avg Packets/Bulk"`
	BwdAvgBulkRate          int     `json:"Bwd Avg Bulk Rate"`
	SubflowFwdPackets       int     `json:"Subflow Fwd Packets"`
	SubflowFwdBytes         int     `json:"Subflow Fwd Bytes"`
	SubflowBwdPackets       int     `json:"Subflow Bwd Packets"`
	SubflowBwdBytes         int     `json:"Subflow Bwd Bytes"`
	InitWinBytesForward     int     `json:"Init_Win_bytes_forward"`
	InitWinBytesBackward    int     `json:"Init_Win_bytes_backward"`
	ActDataPktFwd           int     `json:"act_data_pkt_fwd"`
	MinSegSizeForward       int     `json:"min_seg_size_forward"`
	ActiveMean              float64 `json:"Active Mean"`
	ActiveStd               float64 `json:"Active Std"`
	ActiveMax               int64   `json:"Active Max"`
	ActiveMin               int64   `json:"Active Min"`
	IdleMean                float64 `json:"Idle Mean"`
	IdleStd                 float64 `json:"Idle Std"`
	IdleMax                 int64   `json:"Idle Max"`
	IdleMin                 int64   `json:"Idle Min"`
}


func parseInt(s string) int {
	i, err := strconv.Atoi(s)
	if err != nil {
		log.Printf("Error converting string to int: %s", err)
	}
	return i
}

func parseInt64(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		log.Printf("Error converting string to int64: %s", err)
	}
	return i
}

func main() {

	brokers := []string{"broker1:9092", "broker2:9092", "broker3:9092"}
	topic := "network-traffic"
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to start Sarama producer: %v", err)
	}
	defer producer.Close()

	
	files := []string{"file1.csv", "file2.csv", "file3.csv", "file4.csv", "file5.csv", "file6.csv", "file7.csv", "file8.csv"}

	for _, file := range files {
		processFile(file, producer, topic)
	}
}


func processFile(filePath string, producer sarama.SyncProducer, topic string) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Failed to open file %s: %v", filePath, err)
	}
	defer file.Close()

	reader := csv.NewReader(bufio.NewReader(file))
	_, err = reader.Read() 
		log.Fatalf("Failed to read header from file %s: %v", filePath, err)
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading row: %v", err)
			continue
		}

		data := NetworkTrafficData{
            Unnamed0:                   record[0],
            FlowID:                     record[1],
            SourceIP:                   record[2],
            SourcePort:                 parseInt(record[3]),
            DestinationIP:              record[4],
            DestinationPort:            parseInt(record[5]),
            Protocol:                   parseInt(record[6]),
            Timestamp:                  record[7],
            FlowDuration:               parseInt64(record[8]),
            TotalFwdPackets:            parseInt(record[9]),
            TotalBackwardPackets:       parseInt(record[10]),
            TotalLengthOfFwdPackets:    parseInt64(record[11]),
            TotalLengthOfBwdPackets:    parseInt64(record[12]),
            FwdPacketLengthMax:         parseInt(record[13]),
            FwdPacketLengthMin:         parseInt(record[14]),
            FwdPacketLengthMean:        parseFloat64(record[15]),
            FwdPacketLengthStd:         parseFloat64(record[16]),
            BwdPacketLengthMax:         parseInt(record[17]),
            BwdPacketLengthMin:         parseInt(record[18]),
            BwdPacketLengthMean:        parseFloat64(record[19]),
            BwdPacketLengthStd:         parseFloat64(record[20]),
            FlowBytesPerSecond:         parseFloat64(record[21]),
            FlowPacketsPerSecond:       parseFloat64(record[22]),
            FlowIATMean:                parseFloat64(record[23]),
            FlowIATStd:                 parseFloat64(record[24]),
            FlowIATMax:                 parseInt64(record[25]),
            FlowIATMin:                 parseInt64(record[26]),
            FwdIATTotal:                parseInt64(record[27]),
            FwdIATMean:                 parseFloat64(record[28]),
            FwdIATStd:                  parseFloat64(record[29]),
            FwdIATMax:                  parseInt64(record[30]),
            FwdIATMin:                  parseInt64(record[31]),
            BwdIATTotal:                parseInt64(record[32]),
            BwdIATMean:                 parseFloat64(record[33]),
            BwdIATStd:                  parseFloat64(record[34]),
            BwdIATMax:                  parseInt64(record[35]),
            BwdIATMin:                  parseInt64(record[36]),
            FwdPSHFlags:                parseInt(record[37]),
            BwdPSHFlags:                parseInt(record[38]),
            FwdURGFlags:                parseInt(record[39]),
            BwdURGFlags:                parseInt(record[40]),
            FwdHeaderLength:            parseInt(record[41]),
            BwdHeaderLength:            parseInt(record[42]),
            FwdPacketsPerSecond:        parseFloat64(record[43]),
            BwdPacketsPerSecond:        parseFloat64(record[44]),
            MinPacketLength:            parseInt(record[45]),
            MaxPacketLength:            parseInt(record[46]),
            PacketLengthMean:           parseFloat64(record[47]),
            PacketLengthStd:            parseFloat64(record[48]),
            PacketLengthVariance:       parseFloat64(record[49]),
            FINFlagCount:               parseInt(record[50]),
            SYNFlagCount:               parseInt(record[51]),
            RSTFlagCount:               parseInt(record[52]),
            PSHFlagCount:               parseInt(record[53]),
            ACKFlagCount:               parseInt(record[54]),
            URGFlagCount:               parseInt(record[55]),
            CWEFlagCount:               parseInt(record[56]),
            ECEFlagCount:               parseInt(record[57]),
            DownUpRatio:                parseInt(record[58]),
            AveragePacketSize:          parseFloat64(record[59]),
            AvgFwdSegmentSize:          parseFloat64(record[60]),
            AvgBwdSegmentSize:          parseFloat64(record[61]),
            FwdHeaderLength1:           parseInt(record[62]),
            FwdAvgBytesBulk:            parseInt(record[63]),
            FwdAvgPacketsBulk:          parseInt(record[64]),
            FwdAvgBulkRate:             parseInt(record[65]),
            BwdAvgBytesBulk:            parseInt(record[66]),
            BwdAvgPacketsBulk:          parseInt(record[67]),
            BwdAvgBulkRate:             parseInt(record[68]),
            SubflowFwdPackets:          parseInt(record[69]),
            SubflowFwdBytes:            parseInt(record[70]),
            SubflowBwdPackets:          parseInt(record[71]),
            SubflowBwdBytes:            parseInt(record[72]),
            InitWinBytesForward:        parseInt(record[73]),
            InitWinBytesBackward:       parseInt(record[74]),
            ActDataPktFwd:              parseInt(record[75]),
            MinSegSizeForward:          parseInt(record[76]),
            ActiveMean:                 parseFloat64(record[77]),
            ActiveStd:                  parseFloat64(record[78]),
            ActiveMax:                  parseInt64(record[79]),
            ActiveMin:                  parseInt64(record[80]),
            IdleMean:                   parseFloat64(record[81]),
            IdleStd:                    parseFloat64(record[82]),
            IdleMax:                    parseInt64(record[83]),
            IdleMin:                    parseInt64(record[84]),
        }
        

		jsonData, err := json.Marshal(data)
		if err != nil {
			log.Printf("Failed to marshal data: %v", err)
			continue
		}

		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(jsonData),
		}
		_, _, err = producer.SendMessage(msg)
		if err != nil {
			log.Printf("Failed to send message: %v", err)
		} else {
			fmt.Println("Message sent: ", string(jsonData))
		}
	}
}

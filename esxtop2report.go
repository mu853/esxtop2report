package main
import (
    "fmt"
    "encoding/csv"
    "io"
    "os"
    "log"
    "regexp"
    "strings"
    "path/filepath"
    "strconv"
)

func get_header_pattern() string {
    pattern_list := []string{
        `.*\\Virtual Disk\(Raid\d_\d{2}(_CD)?\)\\Commands/sec`,
        `.*\\Group Cpu\(1:system\)\\% Used`,
        `.*\\Memory\\Kernel MBytes`,
        `.*\\VSAN\(Owner\)\\Avg (Read|Write) Latency in ms`,
        `.*\\Network Port\(vSwitch2:\d+:vmnic[27]\)\\MBits Transmitted/sec`,
    }
    arr := []string{}
    for _, x := range pattern_list {
        arr = append(arr, ("(" + x + ")"))
    }
    return strings.Join(arr, "|")
}

func index(arr []string, s string) int {
    for i, x := range arr {
        if x == s {
            return i
        }
    }
    return -1
}

func get_test_case_name(fileName string) string {
    tmp := strings.Split(fileName, `\`)
    tmp = strings.Split(tmp[len(tmp) - 1], `_`)
    if len(tmp) >= 5 {
        return strings.Join(tmp[2:4], "_")
    }
    return "";
}

func convert_csv(inputFile string, pattern *regexp.Regexp) []string {
    fmt.Printf("inputFile = " + inputFile + "\n")
    
    fr, err := os.Open(inputFile)
    if err != nil {
        log.Fatal("Error:", err)
    }
    defer fr.Close()
    
    r := csv.NewReader(fr)
    
    header, err := r.Read()
    if err != nil {
        log.Fatal("Error:", err)
    }
    target := []string{}
    for _, h := range header {
        if pattern.MatchString(h) {
            target = append(target, h)
        }
    }
    
    testCaseName := get_test_case_name(inputFile)
    
    result := []string{}
    time := 0
    for {
        row, err := r.Read()
        if err == io.EOF {
            break
        } else if err != nil {
            //log.Fatal("Error:", err)
        }
        
        if len(row) < len(header) {
            continue
        }
        
        for _, t := range target {
            cols   := strings.Split(t, `\`)
            host   := strings.Split(cols[2], `.`)[0]
            entity := cols[3]
            metric := cols[4]
            value  := row[index(header, t)]
            cols = []string{ host, testCaseName, strconv.Itoa(time), entity, metric, value }
            result = append(result, "\"" + strings.Join(cols, "\",\"") + "\"\n")
        }
        
        time = time + 20
    }
    
    return result
}

func Map(f func(string, *regexp.Regexp) []string, file string, pattern *regexp.Regexp) chan []string {
    ch := make(chan []string)
    go (func() {
        ch <- f(file, pattern)
    })()
    return ch
}

func main() {
    programName := os.Args[0]
    if len(os.Args) < 3 {
        tmp := strings.Split(programName, `\`)
        log.Fatal("Usage: " + tmp[len(tmp) - 1] + " inputfilename outputfilename")
        os.Exit(1)
    }
    inputFileName := os.Args[1]
    ouputFileName := os.Args[2]
    
    inputFiles, err := filepath.Glob(inputFileName)
    if err != nil {
        log.Fatal("hoge1")
        os.Exit(1)
    }
    
    pattern := regexp.MustCompile(get_header_pattern())
    result := []string{}
    result = append(result, "#TYPE System.Management.Automation.PSCustomObject\n")
    result = append(result, "\"Host\",\"Case\",\"Time\",\"Target\",\"Metric\",\"Value\"\n")
    
    channels := make([]chan []string, len(inputFiles))
    for i, inputFile := range inputFiles {
        channels[i] = Map(convert_csv, inputFile, pattern)
    }
    
    for _, ch := range channels {
        for _, row := range <-ch {
            result = append(result, row)
        }
    }
    
    fw, err := os.Create(ouputFileName)
    if err != nil {
        log.Fatal("Error:", err)
    }
    defer fw.Close()
    
    for _, line := range result {
        fw.Write(([]byte)(line))
    }
}

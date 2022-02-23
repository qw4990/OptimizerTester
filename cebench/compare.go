package cebench

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
)

func RunCECompare(inOpt *InputOption, otherOpt *OtherOption) error {
	jsonLocations := inOpt.JSONPaths
	outDir := otherOpt.OutPath
	needDedup = otherOpt.Dedup
	labels := otherOpt.Labels
	var allEstInfosSlice []EstInfos
	var estInfoMapSlice []map[string]EstInfos
	allInfoTp := make(map[string]struct{})
	for _, location := range jsonLocations {
		var allEstInfos EstInfos
		tmpEstInfoMap := make(map[string]EstInfos)
		jsonBytes, err := ioutil.ReadFile(location)
		if err != nil {
			panic(err)
		}
		err = json.Unmarshal(jsonBytes, &tmpEstInfoMap)
		if err != nil {
			panic(err)
		}
		for _, infos := range tmpEstInfoMap {
			for _, info := range infos {
				allEstInfos = append(allEstInfos, info)
			}
		}
		if needDedup {
			allEstInfos = DedupEstInfo(allEstInfos)
		}
		estInfoMap := make(map[string]EstInfos)
		for _, info := range allEstInfos {
			if _, ok := allInfoTp[info.Type]; !ok {
				allInfoTp[info.Type] = struct{}{}
			}
			estInfoMap[info.Type] = append(estInfoMap[info.Type], info)
		}
		CalcPError(estInfoMap)
		for _, infos := range estInfoMap {
			sort.Sort(infos)
		}
		sort.Sort(allEstInfos)

		allEstInfosSlice = append(allEstInfosSlice, allEstInfos)
		estInfoMapSlice = append(estInfoMapSlice, estInfoMap)
	}

	err := os.MkdirAll(outDir, os.ModePerm)
	if err != nil {
		panic(err)
	}
	reportF, err := os.Create(filepath.Join(outDir, reportMDFile))
	if err != nil {
		panic(err)
	}
	defer func() {
		err = reportF.Close()
		if err != nil {
			panic(err)
		}
	}()

	WriteCompareToFileForInfos(allEstInfosSlice, labels, "All", outDir, reportF)

	for tp := range allInfoTp {
		tmpEstInfosSlice := make([]EstInfos, len(estInfoMapSlice))
		for i, estInfoMap := range estInfoMapSlice {
			tmpEstInfos, ok := estInfoMap[tp]
			if !ok {
				tmpEstInfos = make(EstInfos, 0)
			}
			tmpEstInfosSlice[i] = tmpEstInfos
		}
		WriteCompareToFileForInfos(tmpEstInfosSlice, labels, tp, outDir, reportF)
	}

	fmt.Printf("[%s] Analyze finished and results are written into files. Tester exited.\n", logTime())
	return nil
}

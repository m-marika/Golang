package export

import (
	"ngs-core/graph/conv"
	"ngs-core/graph/model"
	"path"
)

type xlsxTemplatePathResolver struct {
	templateRootDir string
}

func newXlsxTemplatePathResolver(templateRootDir string) xlsxTemplatePathResolver {
	return xlsxTemplatePathResolver{templateRootDir: templateRootDir}
}

func (r *xlsxTemplatePathResolver) resolveForSeqRun() string {
	return r.resolve(model.ELabStepSequencerRun, nil, "")
}

func (r *xlsxTemplatePathResolver) resolveForLabTask(task *model.LabTask, dnaNormalizationSolution string) string {
	return r.resolve(task.Step, task.TableUsed, dnaNormalizationSolution)
}

func (r *xlsxTemplatePathResolver) resolveForRequest(request *model.Request) string {
	return path.Join(r.templateRootDir,"request.xlsx")
}

func (r *xlsxTemplatePathResolver) resolveForClient(client *model.Client) string {
	return path.Join(r.templateRootDir,"client.xlsx")
}

func (r *xlsxTemplatePathResolver) resolve(step model.ELabStep, labTaskWithPlate *bool, dnaNormalizationSolution string) string {
	switch step {
	case model.ELabStepSequencerRun:
		return path.Join(r.templateRootDir, "sequencer_runs.xlsx")
	case model.ELabStepVcf:
		return path.Join(r.templateRootDir, "vcf.xlsx")
	case model.ELabStepLibrary:
		if conv.PtrToBool(labTaskWithPlate) {
			switch dnaNormalizationSolution{
				case "WATER":
					return path.Join(r.templateRootDir, "libraries_with_plate_water_V1.3.xlsx")
				case "TE":
					return path.Join(r.templateRootDir, "libraries_with_plate_TE_V1.3.xlsx")
				default:
					return path.Join(r.templateRootDir, "libraries_with_plate.xlsx")
			}
		} else {
			switch dnaNormalizationSolution{
			case "WATER":
				return path.Join(r.templateRootDir, "libraries_water_V1.3.xlsx")
			case "TE":
				return path.Join(r.templateRootDir, "libraries_TE_V1.3.xlsx")
			default:
				return path.Join(r.templateRootDir, "libraries.xlsx")
			}
		}
	case model.ELabStepDna:
		if conv.PtrToBool(labTaskWithPlate) {
			return path.Join(r.templateRootDir, "dna_with_plate.xlsx")
		} else {
			return path.Join(r.templateRootDir, "dna.xlsx")
		}
	case model.ELabStepAliq:
		return path.Join(r.templateRootDir, "aliq.xlsx")
	default:
		return "template.xlsx"
	}
}
